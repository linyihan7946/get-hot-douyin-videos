import asyncio
import json
import re
import argparse
import os
import sys
import collections
import math
import jieba
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


def is_valid_comment(text):
    """
    过滤无效的评论内容（UI元素、时间地点标记等）
    """
    if not text or len(text.strip()) < 5:
        return False

    text = text.strip()

    # 过滤展开回复相关
    if re.match(r"^展开\s*\d*\s*条回复?$", text):
        return False

    # 过滤时间地点标记 (如: "1天前·北京", "2小时前·广东", "3分钟前·上海")
    if re.match(r"^\d+\s*(秒|分钟|小时|天|周|月|年)前[·\s]?[\u4e00-\u9fa5]*$", text):
        return False

    # 过滤纯数字或纯符号
    if re.match(r"^[\d\s\-\+\*\/\.\,]+$", text):
        return False

    # 过滤只包含英文和数字的短文本（可能是用户ID）
    if len(text) < 8 and re.match(r"^[a-zA-Z0-9_\-\s]+$", text):
        return False

    # 过滤"X小时前·地点"这种格式的变体
    if re.match(r"^\d+小时前", text) and "·" in text:
        return False

    # 过滤"展开X条"格式
    if re.match(r"^展开\d+条", text):
        return False

    return True


def filter_comments(comments, target_count=100):
    """
    过滤评论并返回指定数量的有效评论
    """
    valid_comments = []
    seen = set()

    for comment in comments:
        # 去重
        if comment in seen:
            continue
        seen.add(comment)

        # 验证有效性
        if is_valid_comment(comment):
            valid_comments.append(comment)

        # 达到目标数量就停止
        if len(valid_comments) >= target_count:
            break

    return valid_comments


def simple_cluster_comments(comments, num_clusters=8):
    """
    对评论进行语义聚类，生成有意义的话题标签
    """
    if not comments:
        return {}

    # 话题类别定义：关键词 -> 话题名称
    topic_keywords = {
        # 费用相关
        "token": "Token费用与成本问题",
        "费用": "Token费用与成本问题",
        "收费": "Token费用与成本问题",
        "花钱": "Token费用与成本问题",
        "贵": "Token费用与成本问题",
        "便宜": "Token费用与成本问题",
        "免费": "Token费用与成本问题",
        "省钱": "Token费用与成本问题",
        "价格": "Token费用与成本问题",
        "成本": "Token费用与成本问题",
        "钱": "Token费用与成本问题",
        # 安全相关
        "木马": "安全隐患与隐私担忧",
        "病毒": "安全隐患与隐私担忧",
        "安全": "安全隐患与隐私担忧",
        "风险": "安全隐患与隐私担忧",
        "隐私": "安全隐患与隐私担忧",
        "数据": "安全隐患与隐私担忧",
        "泄露": "安全隐患与隐私担忧",
        "权限": "安全隐患与隐私担忧",
        "肉机": "安全隐患与隐私担忧",
        "工信部": "安全隐患与隐私担忧",
        "央视": "安全隐患与隐私担忧",
        "预警": "安全隐患与隐私担忧",
        "监管": "安全隐患与隐私担忧",
        # 部署安装
        "安装": "部署安装与技术门槛",
        "部署": "部署安装与技术门槛",
        "配置": "部署安装与技术门槛",
        "环境": "部署安装与技术门槛",
        "node": "部署安装与技术门槛",
        "git": "部署安装与技术门槛",
        "gateway": "部署安装与技术门槛",
        "终端": "部署安装与技术门槛",
        "代码": "部署安装与技术门槛",
        "报错": "部署安装与技术门槛",
        "教程": "部署安装与技术门槛",
        # 功能询问
        "能做": "功能与能力询问",
        "可以做": "功能与能力询问",
        "能干": "功能与能力询问",
        "干什么": "功能与能力询问",
        "有什么用": "功能与能力询问",
        "能帮": "功能与能力询问",
        "炒股": "功能与能力询问",
        "赚钱": "功能与能力询问",
        "能": "功能与能力询问",
        "做": "功能与能力询问",
        # 使用门槛
        "听不懂": "学习使用门槛高",
        "看不懂": "学习使用门槛高",
        "不会用": "学习使用门槛高",
        "难": "学习使用门槛高",
        "不懂": "学习使用门槛高",
        "学不会": "学习使用门槛高",
        "老年人": "学习使用门槛高",
        "跟不上": "学习使用门槛高",
        # 硬件要求
        "电脑": "硬件设备要求",
        "显卡": "硬件设备要求",
        "mac": "硬件设备要求",
        "苹果": "硬件设备要求",
        "树莓派": "硬件设备要求",
        "内存": "硬件设备要求",
        # 商业模式
        "卖课": "商业模式与变现",
        "课程": "商业模式与变现",
        "培训": "商业模式与变现",
        "割韭菜": "商业模式与变现",
        "智商税": "商业模式与变现",
        # 产品质疑
        "没用": "对产品价值的质疑",
        "骗局": "对产品价值的质疑",
        "笑话": "对产品价值的质疑",
        "炒作": "对产品价值的质疑",
        "价值": "对产品价值的质疑",
        "忽悠": "对产品价值的质疑",
        # 技术讨论
        "开源": "技术原理讨论",
        "架构": "技术原理讨论",
        "套壳": "技术原理讨论",
        "原理": "技术原理讨论",
        "模型": "技术原理讨论",
        "api": "技术原理讨论",
        # AI影响
        "取代": "AI取代人类的担忧",
        "失业": "AI取代人类的担忧",
        "未来": "AI取代人类的担忧",
        "替代": "AI取代人类的担忧",
        # 实际使用体验
        "好用": "实际使用体验",
        "难用": "实际使用体验",
        "不错": "实际使用体验",
        "厉害": "实际使用体验",
        "试了": "实际使用体验",
        "用了": "实际使用体验",
        "体验": "实际使用体验",
    }

    # 分配评论到话题
    clusters = collections.defaultdict(list)

    for comment in comments:
        matched_topic = None
        max_score = 0

        # 遍历所有关键词，找到最匹配的话题
        for keyword, topic in topic_keywords.items():
            if keyword in comment.lower():
                # 优先匹配更长的关键词
                score = len(keyword)
                if score > max_score:
                    max_score = score
                    matched_topic = topic

        if matched_topic:
            clusters[matched_topic].append(comment)
        else:
            clusters["其他讨论"].append(comment)

    # 按评论数量排序
    sorted_clusters = dict(
        sorted(clusters.items(), key=lambda x: len(x[1]), reverse=True)
    )

    return sorted_clusters


async def close_popups(page):
    """
    尝试关闭常见的抖音弹窗（登录、下载、青少年模式等）
    """
    popups = [
        "[data-e2e='close-icon']",  # 通用关闭图标
        "[data-e2e='login-modal-close']",  # 登录弹窗关闭
        ".dy-account-close",  # 登录弹窗关闭
        ".login-guide-close",  # 登录引导关闭
        ".download-guide-close",  # 下载引导关闭
        ".age-verify-close",  # 年龄验证关闭
        ".dy-pop-close",
        ".dy-guide-close",
        "div[class*='close']",  # 模糊匹配关闭按钮
    ]
    for selector in popups:
        try:
            # 使用较短的超时，避免长时间阻塞
            btn = await page.query_selector(selector)
            if btn and await btn.is_visible():
                await btn.click()
                print(f"  [辅助] 已自动关闭弹窗: {selector}")
        except:
            pass

    # 尝试点击“我知道了”等文本按钮
    try:
        for text in ["我知道了", "不再提示", "关闭"]:
            btn = await page.get_by_role("button", name=text).first
            if await btn.is_visible():
                await btn.click()
                print(f"  [辅助] 已通过文本关闭弹窗: {text}")
    except:
        pass


async def ai_cluster_comments(comments):
    """
    使用 AI 对评论进行总结和聚类 (支持 DashScope 和 Gemini)
    """
    dashscope_key = os.getenv("DASHSCOPE_API_KEY")
    gemini_key = os.getenv("GEMINI_API_KEY")

    if not (dashscope_key or gemini_key) or not comments:
        return None

    prompt = f"""
    以下是关于某个话题的抖音评论，请对这些评论进行归纳总结，提取出 3-5 个核心话题，并将评论分配到对应话题下。
    返回 JSON 格式，格式如下：
    {{"话题1": ["评论A", "评论B"], "话题2": ["评论C"]}}
    
    注意：只返回 JSON，不要有任何解释。
    
    评论内容：
    {chr(10).join(comments[:80])}
    """

    try:
        # 优先使用 DashScope (OpenAI 兼容模式)
        if dashscope_key:
            from openai import AsyncOpenAI

            client = AsyncOpenAI(
                api_key=dashscope_key,
                base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
            )
            response = await client.chat.completions.create(
                model=os.getenv("DASHSCOPE_MODEL", "qwen-plus"),
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            content = response.choices[0].message.content
            if content:
                return json.loads(content)

        # 备选使用 Gemini
        elif gemini_key:
            from google import genai
            from google.genai import types

            client = genai.Client(api_key=gemini_key)
            model_id = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

            response = client.models.generate_content(
                model=model_id,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                ),
            )
            if response.text:
                return json.loads(response.text)

        return None

    except Exception as e:
        print(f"  [AI 聚类错误] {e}")
        return None


async def fetch_video_comments(context, video, semaphore, target_comments=100):
    async with semaphore:
        page = await context.new_page()
        print(f"  [并发] 正在抓取: {video['url']}")

        comments_list = []
        raw_comments = []  # 存储原始评论用于过滤

        async def handle_comment_response(response):
            url = response.url.lower()
            if "comment/list" in url or "aweme/comment" in url:
                try:
                    content_type = response.headers.get("content-type", "")
                    if "json" in content_type:
                        data = await response.json()
                        items = data.get("comments", [])
                        if isinstance(items, list):
                            for item in items:
                                text = item.get("text", "")
                                if text and text not in raw_comments:
                                    raw_comments.append(text)
                except:
                    pass

        page.on("response", handle_comment_response)
        try:
            await page.goto(video["url"], wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3000)

            # 尝试滚动到评论区
            try:
                comment_section = await page.query_selector(
                    '[class*="comment"], [data-e2e="comment"]'
                )
                if comment_section:
                    await comment_section.scroll_into_view_if_needed()
                    await page.wait_for_timeout(2000)
            except:
                pass

            last_count = 0
            no_change_count = 0

            for i in range(400):  # 滚动次数
                if i % 5 == 0:
                    await close_popups(page)

                # 滚动页面和评论区
                await page.evaluate("""
                    (() => {
                        window.scrollBy(0, 150);
                        document.querySelectorAll('div').forEach(el => {
                            try {
                                const style = window.getComputedStyle(el);
                                if (style.overflowY === 'auto' || style.overflowY === 'scroll') {
                                    el.scrollTop += 200;
                                }
                            } catch(e) {}
                        });
                    })()
                """)
                await page.wait_for_timeout(100)

                # 每隔一段时间从DOM提取评论
                if i % 4 == 0:
                    try:
                        dom_comments = await page.evaluate("""
                            () => {
                                const comments = [];
                                const selectors = [
                                    '[class*="comment-text"]',
                                    '[class*="commentText"]',
                                    '[class*="commentContent"]',
                                    '[class*="comment"] span',
                                    '[data-e2e="comment"]'
                                ];
                                for (const selector of selectors) {
                                    const elements = document.querySelectorAll(selector);
                                    elements.forEach(el => {
                                        const text = el.textContent.trim();
                                        if (text && text.length > 5 && !comments.includes(text)) {
                                            comments.push(text);
                                        }
                                    });
                                }
                                return comments;
                            }
                        """)
                        for c in dom_comments:
                            if c not in raw_comments:
                                raw_comments.append(c)
                    except:
                        pass

                # 过滤并统计有效评论数
                comments_list = filter_comments(raw_comments, target_comments)

                # 当有效评论达到目标时停止
                if len(comments_list) >= target_comments:
                    break

                if len(raw_comments) == last_count:
                    no_change_count += 1
                else:
                    no_change_count = 0

                if no_change_count >= 80:  # 增加无变化阈值
                    break
                last_count = len(raw_comments)

            video["comments"] = comments_list[:target_comments]
            print(f"  [完成] {video['url']} | 抓取到 {len(video['comments'])} 条评论")
        except Exception as e:
            print(f"  [错误] 抓取 {video['url']} 失败: {str(e)}")
        finally:
            await page.close()


async def get_hot_douyin_videos(
    keyword,
    max_videos=10,
    headless=False,
    concurrency=3,
    skip_cluster=False,
    start_date=None,
    end_date=None,
):
    async with async_playwright() as p:
        user_data_path = os.path.join(os.getcwd(), "user_data")
        context = await p.chromium.launch_persistent_context(
            user_data_dir=user_data_path,
            headless=headless,
            viewport={"width": 1366, "height": 768},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        page = context.pages[0] if context.pages else await context.new_page()

        print("\n" + "=" * 50)
        print("-> 正在打开抖音网页版...")
        if not headless:
            print("-> 抖音有严格的反爬机制。如果是首次运行，请务必扫码登录抖音。")
        print("=" * 50 + "\n")

        await page.goto("https://www.douyin.com")

        cookies = await context.cookies()
        is_logged_in = any(
            "session" in str(c.get("name")) or "passport" in str(c.get("name"))
            for c in cookies
        )

        if not is_logged_in:
            if headless:
                print("-> [警告] 无头模式下未检测到登录，建议先用有头模式登录一次。")
            else:
                print("-> 【请在弹出的浏览器中完成扫码登录】")
                await asyncio.to_thread(
                    input, "-> 登录成功后，请在此终端按【回车键】继续: "
                )

        print(f"\n-> 开始搜索课题: {keyword}")
        videos_data = {}

        async def handle_search_response(response):
            if "aweme/v1/web/general/search/single/" in response.url:
                try:
                    data = await response.json()
                    for item in data.get("data", []):
                        if item.get("type") == 1 and "aweme_info" in item:
                            aweme = item["aweme_info"]
                            vid = aweme.get("aweme_id")
                            if not vid:
                                continue
                            stats = aweme.get("statistics", {})
                            create_time = aweme.get("create_time", 0)
                            videos_data[vid] = {
                                "video_id": vid,
                                "url": f"https://www.douyin.com/video/{vid}",
                                "description": aweme.get("desc", "无描述"),
                                "likes": stats.get("digg_count", 0),
                                "shares": stats.get("share_count", 0),
                                "comments_count": stats.get("comment_count", 0),
                                "create_time": create_time,
                                "create_time_str": datetime.fromtimestamp(
                                    create_time
                                ).strftime("%Y-%m-%d %H:%M:%S")
                                if create_time
                                else "未知",
                                "comments": [],
                            }
                except:
                    pass

        page.on("response", handle_search_response)
        search_url = (
            f"https://www.douyin.com/search/{keyword}?source=normal_search&type=general"
        )
        await page.goto(search_url)

        print("-> 正在抓取搜索结果...")
        try:
            await page.wait_for_selector(
                "[data-e2e='search_result_list']", timeout=15000
            )
        except:
            print("-> [提示] 等待搜索结果列表超时，尝试继续...")

        for i in range(8):  # 增加滚动次数以确保拿到更多视频
            if i % 3 == 0:
                await close_popups(page)
            await page.mouse.wheel(0, 1200)
            await page.wait_for_timeout(1000)
            if len(videos_data) >= max_videos + 5:
                break

        page.remove_listener("response", handle_search_response)

        if not videos_data:
            print("-> [警告] 未获取到视频数据。")
            await context.close()
            return

        time_filter_info = ""
        if start_date or end_date:
            start_ts = (
                datetime.strptime(start_date, "%Y-%m-%d").timestamp()
                if start_date
                else 0
            )
            end_ts = (
                datetime.strptime(end_date, "%Y-%m-%d").timestamp() + 86400
                if end_date
                else datetime.now().timestamp()
            )

            filtered_videos = {
                vid: v
                for vid, v in videos_data.items()
                if v.get("create_time", 0) >= start_ts
                and v.get("create_time", 0) <= end_ts
            }
            print(
                f"-> 时间范围过滤: {len(videos_data)} -> {len(filtered_videos)} 个视频"
            )
            time_filter_info = (
                f" (时间范围: {start_date or '不限'} ~ {end_date or '不限'})"
            )
            videos_data = filtered_videos

        if not videos_data:
            print("-> [警告] 指定时间范围内未找到视频。")
            await context.close()
            return

        sorted_videos = sorted(
            videos_data.values(), key=lambda x: x["likes"], reverse=True
        )
        top_videos = sorted_videos[:max_videos]

        print(
            f"\n-> 选取点赞前 {len(top_videos)} 个视频{time_filter_info}，开始并发抓取评论 (并发数: {concurrency})..."
        )

        semaphore = asyncio.Semaphore(concurrency)
        tasks = [
            fetch_video_comments(context, video, semaphore, target_comments=100)
            for video in top_videos
        ]
        await asyncio.gather(*tasks)

        # 3. 评论聚类（可选）
        if not skip_cluster:
            for video in top_videos:
                if video["comments"]:
                    video["comment_clusters"] = simple_cluster_comments(
                        video["comments"]
                    )
                    print(f"\n[视频聚类分析] {video['url']}")
                    for cluster, items in video["comment_clusters"].items():
                        sample = items[0][:40] if items else "无"
                        clean_sample = re.sub(r"[^\x20-\x7E\u4E00-\u9FFF]", "", sample)
                        print(f"  - {cluster} ({len(items)}条): {clean_sample}...")

        # 4. 保存结果
        output_data = {
            "keyword": keyword,
            "time_range": {"start_date": start_date, "end_date": end_date},
            "videos": top_videos,
        }
        output_file = f"douyin_result_{keyword}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=4)

        # 5. 保存文本格式结果
        txt_file = f"douyin_result_{keyword}.txt"
        with open(txt_file, "w", encoding="utf-8") as f:
            f.write(f"关键词: {keyword}\n")
            if start_date or end_date:
                f.write(f"时间范围: {start_date or '不限'} ~ {end_date or '不限'}\n")
            f.write(f"视频数量: {len(top_videos)}\n")
            f.write("=" * 60 + "\n\n")

            for i, video in enumerate(top_videos, 1):
                f.write(f"【视频 {i}】\n")
                f.write(f"视频描述: {video.get('description', '无描述')}\n")
                f.write(f"视频链接: {video.get('url', '无')}\n")
                f.write(f"点赞数: {video.get('likes', 0)}\n")
                f.write(f"分享数: {video.get('shares', 0)}\n")
                f.write(f"评论数: {video.get('comments_count', 0)}\n")
                if video.get("create_time_str"):
                    f.write(f"发布时间: {video.get('create_time_str')}\n")

                if video.get("comment_clusters"):
                    f.write("\n评论聚类:\n")
                    for cluster, items in video["comment_clusters"].items():
                        f.write(f"  [{cluster}] ({len(items)}条)\n")
                        for item in items[:3]:
                            clean_item = re.sub(r"[^\x20-\x7E\u4E00-\u9FFF]", "", item)[
                                :60
                            ]
                            f.write(f"    - {clean_item}\n")
                        if len(items) > 3:
                            f.write(f"    ... 共 {len(items)} 条\n")
                f.write("\n" + "-" * 60 + "\n\n")

        print(f"\n-> 任务完成！结果已保存到 {output_file} 和 {txt_file}")
        await context.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="抖音热门视频与评论抓取聚类工具")
    parser.add_argument(
        "-k", "--keyword", type=str, required=True, help="搜索关键词（必填）"
    )
    parser.add_argument(
        "-m", "--max", type=int, default=10, help="最大视频数量（默认: 10）"
    )
    parser.add_argument("--headless", action="store_true", help="无头模式运行")
    parser.add_argument(
        "-c", "--concurrency", type=int, default=3, help="并发抓取数（默认: 3）"
    )
    parser.add_argument("--no-cluster", action="store_true", help="跳过评论聚类分析")
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="开始日期，格式 YYYY-MM-DD（默认: 不限）",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="结束日期，格式 YYYY-MM-DD（默认: 不限）",
    )

    args = parser.parse_args()
    asyncio.run(
        get_hot_douyin_videos(
            args.keyword,
            args.max,
            args.headless,
            args.concurrency,
            args.no_cluster,
            args.start_date,
            args.end_date,
        )
    )

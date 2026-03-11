import asyncio
import json
import re
import argparse
import os
import sys
import collections
import math
import jieba
from playwright.async_api import async_playwright

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


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


async def fetch_video_comments(context, video, semaphore, target_comments=200):
    async with semaphore:
        page = await context.new_page()
        print(f"  [并发] 正在抓取: {video['url']}")

        comments_list = []

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
                                if text and text not in comments_list:
                                    comments_list.append(text)
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

            for i in range(300):  # 增加滚动次数
                if i % 5 == 0:
                    await close_popups(page)

                # 滚动页面
                await page.evaluate("""
                    (() => {
                        window.scrollBy(0, 150);
                        document.querySelectorAll('div').forEach(el => {
                            try {
                                const style = window.getComputedStyle(el);
                                if (style.overflowY === 'auto' || style.overflowY === 'scroll') {
                                    el.scrollTop += 150;
                                }
                            } catch(e) {}
                        });
                    })()
                """)
                await page.wait_for_timeout(150)

                # 每隔一段时间从DOM提取评论
                if i % 8 == 0:
                    try:
                        dom_comments = await page.evaluate("""
                            () => {
                                const comments = [];
                                const selectors = [
                                    '[class*="comment-text"]',
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
                            if c not in comments_list:
                                comments_list.append(c)
                    except:
                        pass

                current_count = len(comments_list)
                if current_count >= target_comments:
                    break

                if current_count == last_count:
                    no_change_count += 1
                else:
                    no_change_count = 0

                if no_change_count >= 50:
                    break
                last_count = current_count

            video["comments"] = comments_list[:target_comments]
            print(f"  [完成] {video['url']} | 抓取到 {len(video['comments'])} 条评论")
        except Exception as e:
            print(f"  [错误] 抓取 {video['url']} 失败: {str(e)}")
        finally:
            await page.close()


async def get_hot_douyin_videos(
    keyword, max_videos=10, headless=False, concurrency=3, skip_cluster=False
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
                            videos_data[vid] = {
                                "video_id": vid,
                                "url": f"https://www.douyin.com/video/{vid}",
                                "description": aweme.get("desc", "无描述"),
                                "likes": stats.get("digg_count", 0),
                                "shares": stats.get("share_count", 0),
                                "comments_count": stats.get("comment_count", 0),
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

        sorted_videos = sorted(
            videos_data.values(), key=lambda x: x["likes"], reverse=True
        )
        top_videos = sorted_videos[:max_videos]

        print(
            f"\n-> 选取点赞前 {len(top_videos)} 个视频，开始并发抓取评论 (并发数: {concurrency})..."
        )

        semaphore = asyncio.Semaphore(concurrency)
        tasks = [
            fetch_video_comments(context, video, semaphore, target_comments=200)
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
        output_data = {"keyword": keyword, "videos": top_videos}
        output_file = f"douyin_result_{keyword}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=4)

        print(f"\n-> 任务完成！结果已保存到 {output_file}")
        await context.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="抖音热门视频与评论抓取聚类工具")
    parser.add_argument("-k", "--keyword", type=str, required=True, help="关键词")
    parser.add_argument("-m", "--max", type=int, default=10, help="最大视频数量")
    parser.add_argument("--headless", action="store_true", help="无头模式")
    parser.add_argument("-c", "--concurrency", type=int, default=3, help="并发数")
    parser.add_argument(
        "--no-cluster", action="store_true", help="跳过聚类（可用大模型手动分析）"
    )

    args = parser.parse_args()
    asyncio.run(
        get_hot_douyin_videos(
            args.keyword, args.max, args.headless, args.concurrency, args.no_cluster
        )
    )

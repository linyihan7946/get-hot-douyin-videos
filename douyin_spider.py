import asyncio
import json
import re
import argparse
import os
import sys
from playwright.async_api import async_playwright

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

try:
    from google import genai
    from google.genai import types
except ImportError:
    genai = None


def cluster_comments_with_llm(comments, api_key, base_url, model):
    if not comments:
        print("-> [跳过] 没有提取到任何评论，无法进行聚类分析。")
        return None

    print(
        f"\n-> 正在使用 LLM ({model}) 对提取到的 {len(comments)} 条评论进行语义聚类分析..."
    )

    prompt = """
你是一个专业的数据分析专家。请对以下抖音热门视频的评论按“相近意思/语义”进行分类聚类。
要求：
1. 归纳出不超过20个主要类别。
2. 类别名称要精简（例如：“认为价格太贵/消耗token大”、“寻求教程/求带”、“觉得没意义/智商税”等）。
3. 统计每个类别的评论数量，并按数量从多到少排序。
4. 每个类别提供2-3个真实的评论样例。
5. 请只返回合法的 JSON 数组格式，不要输出任何其他的解释文字、Markdown标记或代码块（如 ```json）。

JSON格式示例：
[
    {
        "category": "认为价格太贵/消耗token大",
        "count": 45,
        "examples": ["太贵了用不起", "一晚上烧了我一万token"]
    }
]

待分类的评论列表如下：
"""
    # 为了避免超过上下文，稍微截断
    prompt += json.dumps(comments[:300], ensure_ascii=False)

    try:
        # Check if using Gemini model
        if "gemini" in model.lower():
            if not genai:
                print(
                    "-> [错误] 未安装 google-genai 库，请执行 `pip install google-genai` 安装。"
                )
                return None

            client = genai.Client(api_key=api_key)
            response = client.models.generate_content(
                model=model,
                contents=prompt,
                config={
                    "system_instruction": "你是一个只输出 JSON 格式数据的机器。",
                    "temperature": 0.3,
                },
            )
            content = response.text.strip() if response.text else ""
        else:
            if not OpenAI:
                print("-> [错误] 未安装 openai 库，请执行 `pip install openai` 安装。")
                return None

            client = OpenAI(api_key=api_key, base_url=base_url)
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {
                        "role": "system",
                        "content": "你是一个只输出 JSON 格式数据的机器。",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.3,
            )
            content = response.choices[0].message.content.strip()

        # 尝试清洗潜在的 markdown 块
        if content.startswith("```"):
            content = re.sub(
                r"^```(json)?|```$", "", content, flags=re.MULTILINE
            ).strip()

        result = json.loads(content)
        return result
    except Exception as e:
        print(f"-> [错误] 调用 LLM 失败: {e}")
        return None

    if not comments:
        print("-> [跳过] 没有提取到任何评论，无法进行聚类分析。")
        return None

    print(
        f"\n-> 正在使用 LLM ({model}) 对提取到的 {len(comments)} 条评论进行语义聚类分析..."
    )

    client = OpenAI(api_key=api_key, base_url=base_url)

    prompt = """
你是一个专业的数据分析专家。请对以下抖音热门视频的评论按“相近意思/语义”进行分类聚类。
要求：
1. 归纳出不超过20个主要类别。
2. 类别名称要精简（例如：“认为价格太贵/消耗token大”、“寻求教程/求带”、“觉得没意义/智商税”等）。
3. 统计每个类别的评论数量，并按数量从多到少排序。
4. 每个类别提供2-3个真实的评论样例。
5. 请只返回合法的 JSON 数组格式，不要输出任何其他的解释文字、Markdown标记或代码块（如 ```json）。

JSON格式示例：
[
    {
        "category": "认为价格太贵/消耗token大",
        "count": 45,
        "examples": ["太贵了用不起", "一晚上烧了我一万token"]
    }
]

待分类的评论列表如下：
"""
    # 为了避免超过上下文，稍微截断
    prompt += json.dumps(comments[:300], ensure_ascii=False)

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "你是一个只输出 JSON 格式数据的机器。"},
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
        )
        content = response.choices[0].message.content.strip()

        # 尝试清洗潜在的 markdown 块
        if content.startswith("```"):
            content = re.sub(
                r"^```(json)?|```$", "", content, flags=re.MULTILINE
            ).strip()

        result = json.loads(content)
        return result
    except Exception as e:
        print(f"-> [错误] 调用 LLM 失败: {e}")
        return None


async def get_hot_douyin_videos(
    keyword, max_videos=5, api_key=None, base_url=None, model=None
):
    async with async_playwright() as p:
        # 启动带有持久化数据的 Chromium 浏览器上下文
        user_data_path = os.path.join(os.getcwd(), "user_data")
        context = await p.chromium.launch_persistent_context(
            user_data_dir=user_data_path,
            headless=False,
            viewport={"width": 1366, "height": 768},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        page = context.pages[0] if context.pages else await context.new_page()

        print("\n" + "=" * 50)
        print("-> 正在打开抖音网页版...")
        print("-> 抖音有严格的反爬机制。如果是首次运行，请务必扫码登录抖音。")
        print("-> 登录成功后，请在终端按回车键继续（无需每次都登录，视抖音策略而定）。")
        print("=" * 50 + "\n")

        await page.goto("https://www.douyin.com")

        # 检查是否已经有登录态（通过判断 cookie 中是否包含 sessionid 或 passport 相关字眼）
        cookies = await context.cookies()
        is_logged_in = any(
            "session" in str(c.get("name")) or "passport" in str(c.get("name"))
            for c in cookies
        )

        if is_logged_in:
            print("-> 检测到已保存的登录状态，自动跳过扫码环节！")
            await page.wait_for_timeout(3000)
        else:
            print("-> 未检测到登录状态或首次运行。")
            print("-> 【请在弹出的浏览器中完成扫码登录】")
            # 阻塞在此，等待用户在终端按下回车键（确认登录完成）
            await asyncio.to_thread(
                input, "-> 登录成功后，请在此终端按【回车键】继续: "
            )

        print(f"\n-> 开始搜索课题: {keyword}")

        # 存储所有截获到的视频数据
        videos_data = {}

        # 1. 设置网络拦截，获取搜索接口返回的内容
        async def handle_search_response(response):
            if "aweme/v1/web/general/search/single/" in response.url:
                try:
                    data = await response.json()
                    for item in data.get("data", []):
                        # 判断是否为视频内容 (type=1) 且包含 aweme_info
                        if item.get("type") == 1 and "aweme_info" in item:
                            aweme = item["aweme_info"]
                            vid = aweme.get("aweme_id")
                            if not vid:
                                continue

                            desc = aweme.get("desc", "无描述")
                            stats = aweme.get("statistics", {})
                            likes = stats.get("digg_count", 0)
                            shares = stats.get("share_count", 0)
                            comments_count = stats.get("comment_count", 0)

                            videos_data[vid] = {
                                "video_id": vid,
                                "url": f"https://www.douyin.com/video/{vid}",
                                "description": desc,
                                "likes": likes,
                                "shares": shares,
                                "comments_count": comments_count,
                                "comments": [],
                            }
                except Exception as e:
                    pass

        # 绑定搜索接口的拦截事件
        page.on("response", handle_search_response)

        # 导航到搜索页面
        search_url = (
            f"https://www.douyin.com/search/{keyword}?source=normal_search&type=general"
        )
        await page.goto(search_url)

        print("-> 正在抓取搜索结果（程序会自动向下滚动几页加载数据）...")
        # 增加初始加载时间，以防出现验证码需要手动处理
        print("-> 【如果出现验证码，请在15秒内完成验证】...")
        await page.wait_for_timeout(15000)  # 等待初始加载和验证码处理

        # 模拟向下滚动，触发后续的搜索结果加载
        for i in range(5):
            await page.mouse.wheel(0, 1200)
            await page.wait_for_timeout(2000)
            print(f"   已滚动 {i + 1}/5 次...")

        # 移除搜索页的拦截器
        page.remove_listener("response", handle_search_response)

        if not videos_data:
            print("-> [警告] 未获取到任何视频数据。可能是搜索请求失败或被验证码拦截。")
            await context.close()
            return

        # 核心逻辑：将视频数据按点赞量从高到低排序
        sorted_videos = sorted(
            videos_data.values(), key=lambda x: x["likes"], reverse=True
        )
        top_videos = sorted_videos[:max_videos]

        print(
            f"\n-> 成功获取到视频列表，选取点赞量排名前 {len(top_videos)} 的视频，开始抓取评论..."
        )

        # 2. 遍历热门视频，提取评论
        final_results = []
        for index, video in enumerate(top_videos, 1):
            print(f"\n[{index}/{len(top_videos)}] 正在处理视频: {video['url']}")
            print(
                f"  点赞: {video['likes']} | 描述摘要: {video['description'][:30]}..."
            )

            comments_list = []

            # 设置网络拦截，获取评论接口返回的内容
            async def handle_comment_response(response):
                if "aweme/v1/web/comment/list/" in response.url:
                    try:
                        data = await response.json()
                        for comment in data.get("comments", []):
                            text = comment.get("text", "")
                            # 去重并添加
                            if text and text not in comments_list:
                                comments_list.append(text)
                    except Exception as e:
                        pass

            # 绑定评论接口的拦截事件
            page.on("response", handle_comment_response)

            # 跳转到单视频播放页
            await page.goto(video["url"])
            await page.wait_for_timeout(4000)  # 等待页面加载和评论接口首次请求

            # 评论区通常可以通过滚动页面加载更多
            print("  正在滚动加载更多评论...")
            for _ in range(3):
                await page.mouse.wheel(0, 800)
                await page.wait_for_timeout(2000)

            # 移除当前视频的拦截器
            page.remove_listener("response", handle_comment_response)

            # 保存前 50 条评论作为结果
            video["comments"] = comments_list[:50]
            final_results.append(video)

        # 3. 保存最终结果到 JSON 文件
        output_file = f"douyin_result_{keyword}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(final_results, f, ensure_ascii=False, indent=4)

        print(
            f"\n-> 任务完成！原始数据结果已成功保存到当前目录的 {output_file} 文件中。"
        )

        # 4. (新增) 进行 LLM 评论聚类分析
        if api_key:
            # 汇集所有视频收集到的评论去重
            all_comments = []
            for video in final_results:
                for c in video.get("comments", []):
                    if c and c not in all_comments:
                        all_comments.append(c)

            clustered_result = cluster_comments_with_llm(
                all_comments, api_key, base_url, model
            )

            if clustered_result:
                cluster_file = f"douyin_result_{keyword}_clustered.json"
                with open(cluster_file, "w", encoding="utf-8") as f:
                    json.dump(clustered_result, f, ensure_ascii=False, indent=4)

                print(f"-> 评论聚类分析完成！前20大评论分类已保存至 {cluster_file}。")
                print("\n【Top 5 评论类别预览】:")
                for i, item in enumerate(clustered_result[:5], 1):
                    print(f"{i}. [{item['category']}] - 数量: {item['count']}条")
                    print(f"   样例: {item['examples'][0]}")
        else:
            print("\n-> [提示] 未配置大语言模型 API Key，已跳过评论的语义聚类分析。")
            print(
                "   如需将意思相近的评论归类合并，请在运行命令中添加 --api-key 参数。"
            )

        await context.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="抖音热门视频与评论抓取工具")
    parser.add_argument(
        "-k", "--keyword", type=str, required=True, help="要搜索的课题/关键词"
    )
    parser.add_argument(
        "-m", "--max", type=int, default=5, help="要获取的最大视频数量（默认5个）"
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default="",
        help="LLM API Key，填写即开启评论语义聚类分析",
    )
    parser.add_argument(
        "--base-url",
        type=str,
        default="https://api.openai.com/v1",
        help="LLM API Base URL，默认为 OpenAI 官方地址。如用 DeepSeek 请填 https://api.deepseek.com",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="gpt-4o-mini",
        help="LLM 模型名称，默认为 gpt-4o-mini。如果你想使用 Gemini 请填入如 gemini-3.1-pro-preview",
    )

    args = parser.parse_args()

    # 简化逻辑：如果检测到 GEMINI_API_KEY，自动使用 Gemini 模型
    gemini_api_key = os.getenv("GEMINI_API_KEY", "")
    gemini_model = os.getenv("GEMINI_MODEL", "gemini-3.1-pro-preview")
    openai_api_key = os.getenv("OPENAI_API_KEY", "")

    if gemini_api_key:
        # 优先使用 Gemini
        api_key_to_use = gemini_api_key
        model_to_use = (
            args.model if args.model and args.model != "gpt-4o-mini" else gemini_model
        )
        base_url_to_use = ""
    elif openai_api_key:
        # 回退到 OpenAI 兼容接口
        api_key_to_use = openai_api_key
        model_to_use = (
            args.model
            if args.model != "gpt-4o-mini"
            else os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        )
        base_url_to_use = (
            args.base_url
            if args.base_url != "https://api.openai.com/v1"
            else os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
        )
    else:
        # 命令行参数优先级最高
        api_key_to_use = args.api_key if args.api_key else ""
        model_to_use = args.model if args.model != "gpt-4o-mini" else "gpt-4o-mini"
        base_url_to_use = (
            args.base_url
            if args.base_url != "https://api.openai.com/v1"
            else "https://api.openai.com/v1"
        )

    # 运行异步主函数
    asyncio.run(
        get_hot_douyin_videos(
            args.keyword, args.max, api_key_to_use, base_url_to_use, model_to_use
        )
    )

import asyncio
import json
import re
import argparse
from playwright.async_api import async_playwright


async def get_hot_douyin_videos(keyword, max_videos=5):
    async with async_playwright() as p:
        # 启动 Chromium 浏览器，开启可见模式以便于扫码登录和处理滑块验证码
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            viewport={"width": 1366, "height": 768},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        page = await context.new_page()

        print("\n" + "=" * 50)
        print("-> 正在打开抖音网页版...")
        print("-> 抖音有严格的反爬机制。如果是首次运行，请务必扫码登录抖音。")
        print("-> 登录成功后，请在终端按回车键继续（无需每次都登录，视抖音策略而定）。")
        print("=" * 50 + "\n")

        await page.goto("https://www.douyin.com")

        # 阻塞在此，等待用户在终端按下回车键（确认登录完成）
        await asyncio.to_thread(input, "-> 【请在此终端按回车键继续】: ")

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
        await page.wait_for_timeout(3000)  # 等待初始加载

        # 模拟向下滚动，触发后续的搜索结果加载
        for i in range(5):
            await page.mouse.wheel(0, 1200)
            await page.wait_for_timeout(2000)
            print(f"   已滚动 {i + 1}/5 次...")

        # 移除搜索页的拦截器
        page.remove_listener("response", handle_search_response)

        if not videos_data:
            print("-> [警告] 未获取到任何视频数据。可能是搜索请求失败或被验证码拦截。")
            await browser.close()
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

        print(f"\n-> 任务完成！结果已成功保存到当前目录的 {output_file} 文件中。")

        await browser.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="抖音热门视频与评论抓取工具")
    parser.add_argument(
        "-k", "--keyword", type=str, required=True, help="要搜索的课题/关键词"
    )
    parser.add_argument(
        "-m", "--max", type=int, default=5, help="要获取的最大视频数量（默认5个）"
    )

    args = parser.parse_args()

    # 运行异步主函数
    asyncio.run(get_hot_douyin_videos(args.keyword, args.max))

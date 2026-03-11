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


async def get_hot_douyin_videos(keyword, max_videos=10):
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
                if "comment" in response.url and "json" in response.headers.get(
                    "content-type", ""
                ):
                    try:
                        data = await response.json()
                        items = (
                            data.get("comments")
                            or data.get("data", {}).get("comments")
                            or []
                        )
                        if isinstance(items, list):
                            for item in items:
                                text = item.get("text", "")
                                if text and text not in comments_list:
                                    comments_list.append(text)
                    except:
                        pass

            page.on("response", handle_comment_response)
            await page.goto(video["url"])
            await page.wait_for_timeout(4000)

            print("  正在尝试触发评论加载...")
            await page.wait_for_timeout(3000)

            # 尝试定位评论区并进行滚动
            # 抖音网页版评论区通常在右侧或下方，尝试在多个位置模拟滚动
            print("  正在循环滚动加载更多评论（目标 500 条）...")
            max_scrolls = 40
            for i in range(max_scrolls):
                # 尝试在不同位置滚动，并强制滚动所有可能的容器
                await page.evaluate("""
                    (() => {
                        // 1. 滚动主窗口
                        window.scrollBy(0, 1000);
                        
                        // 2. 尝试查找并滚动所有带有滚动条的容器（评论区通常是这种）
                        const scrollables = document.querySelectorAll('div');
                        scrollables.forEach(el => {
                            const style = window.getComputedStyle(el);
                            if (style.overflowY === 'auto' || style.overflowY === 'scroll') {
                                el.scrollTop += 1000;
                            }
                        });
                    })()
                """)

                await page.keyboard.press("PageDown")
                await page.wait_for_timeout(1200)

                if len(comments_list) >= 500:
                    break

                if (i + 1) % 10 == 0:
                    print(f"    当前进度：{len(comments_list)} 条评论...")

            page.remove_listener("response", handle_comment_response)
            video["comments"] = comments_list[:500]
            print(f"  最终获取到 {len(video['comments'])} 条评论。")
            final_results.append(video)

        # 3. 合并数据并保存到同一个 JSON 文件
        output_data = {
            "keyword": keyword,
            "videos": final_results,
        }

        output_file = f"douyin_result_{keyword}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=4)

        print(f"\n-> 任务完成！结果已保存到 {output_file} 文件中。")

        await context.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="抖音热门视频与评论抓取工具")
    parser.add_argument(
        "-k", "--keyword", type=str, required=True, help="要搜索的课题/关键词"
    )
    parser.add_argument(
        "-m", "--max", type=int, default=10, help="要获取的最大视频数量（默认10个）"
    )

    args = parser.parse_args()

    # 运行异步主函数
    asyncio.run(get_hot_douyin_videos(args.keyword, args.max))

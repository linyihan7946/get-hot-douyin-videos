import asyncio
import json
import re
import argparse
import os
import sys
import collections
import math
from playwright.async_api import async_playwright

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


def simple_cluster_comments(comments, num_clusters=5):
    """
    对评论进行简单的聚类（基于高频词）
    """
    if not comments:
        return {}

    def get_tokens(text):
        # 简单清洗并按字符分词（适用于中文）
        return [
            c for c in text.lower() if c.strip() and c not in "，。！？“”‘’（）()[]【】"
        ]

    all_tokens = []
    comment_data = []
    for c in comments:
        tokens = get_tokens(c)
        comment_data.append({"text": c, "tokens": collections.Counter(tokens)})
        all_tokens.extend(tokens)

    if not all_tokens:
        return {"其他": comments}

    # 获取前几个最高频的词作为“话题”中心
    # 过滤掉单字（可选，这里保留以保证通用性）
    counter = collections.Counter(all_tokens)
    common_tokens = [t for t, count in counter.most_common(num_clusters)]

    clusters = collections.defaultdict(list)
    for data in comment_data:
        best_tag = "其他"
        max_overlap = 0
        for tag in common_tokens:
            if data["tokens"][tag] > max_overlap:
                max_overlap = data["tokens"][tag]
                best_tag = tag
        clusters[f"话题: {best_tag}"].append(data["text"])

    return dict(clusters)


async def fetch_video_comments(context, video, semaphore, target_comments=100):
    async with semaphore:
        page = await context.new_page()
        print(f"  [并发] 正在抓取: {video['url']}")

        comments_list = []

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
        try:
            # 增加超时容错
            await page.goto(video["url"], wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3000)

            last_count = 0
            no_change_count = 0

            for i in range(50):  # 增加滚动次数以确保拿到100条
                await page.evaluate("""
                    (() => {
                        window.scrollBy(0, 1000);
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
                await page.wait_for_timeout(800)

                current_count = len(comments_list)
                if current_count >= target_comments:
                    break

                if current_count == last_count:
                    no_change_count += 1
                else:
                    no_change_count = 0

                if no_change_count >= 8:
                    break
                last_count = current_count

            video["comments"] = comments_list[:target_comments]
            print(f"  [完成] {video['url']} | 抓取到 {len(video['comments'])} 条评论")
        except Exception as e:
            print(f"  [错误] 抓取 {video['url']} 失败: {str(e)}")
        finally:
            await page.close()


async def get_hot_douyin_videos(keyword, max_videos=10, headless=False, concurrency=3):
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
            fetch_video_comments(context, video, semaphore, target_comments=100)
            for video in top_videos
        ]
        await asyncio.gather(*tasks)

        # 3. 评论聚类
        for video in top_videos:
            if video["comments"]:
                video["comment_clusters"] = simple_cluster_comments(video["comments"])
                print(f"\n[视频聚类分析] {video['url']}")
                for cluster, items in video["comment_clusters"].items():
                    print(f"  - {cluster} ({len(items)}条): {items[0][:40]}...")

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

    args = parser.parse_args()
    asyncio.run(
        get_hot_douyin_videos(args.keyword, args.max, args.headless, args.concurrency)
    )

# Web Crawling Details

## Types of Crawls

**Limit Web Crawl Options:**

1. **Equal and Below:** This option restricts the scraping to pages whose URLs begin exactly with the specified starting point and includes any subsequent pages that follow this path. For example, choosing `nasa.gov/blogs` will target all blog entries (like `nasa.gov/blogs/new-rocket`), but it will not include unrelated paths such as `nasa.gov/events`. It's like following a branch on a tree without jumping to a different branch.
2. **Same Subdomain:** When you select this option, the scraper will focus on a specific subdomain, collecting data from all the pages within it. For instance, if you choose `docs.nasa.gov`, it will explore all the pages under this subdomain exclusively, ignoring pages on `nasa.gov` or other subdomains like `api.nasa.gov`. Imagine this as confining the scraping to a single section of a library.
3. **Entire Domain:** Opting for this allows the scraper to access all content under the main domain, including its subdomains. Selecting `nasa.gov` means it can traverse through `docs.nasa.gov`, `api.nasa.gov`, and any other subdomains present. Think of it as having a pass to explore every room in a building.
4. **All:** This is the most extensive scraping option, where the scraper begins at your specified URL and ventures out to any linked pages, potentially going beyond the initial domain. It's akin to setting out on a web expedition with no specific boundary.

**Recommendation:** Starting with the "Equal and Below" option is advisable for a focused and manageable scrape. If your needs expand, you can re-run the process with broader options as required.



## Backend Code

Web crawling is powered by [Crawlee.dev](https://crawlee.dev/). Our implementation is [open source on Github](https://github.com/UIUC-Chatbot/crawlee).

Happily, I've seen web scraping take place at 10Gbps, using 6 cores of parallel javascript. It's a performant option even with basic hosting on [Railway.app](https://railway.app/). The baseline 100MB of memory usage costs $1/mo on Railway, pretty nifty.

<figure><img src="../.gitbook/assets/image (3).png" alt=""><figcaption></figcaption></figure>

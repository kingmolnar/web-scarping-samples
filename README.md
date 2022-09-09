# Web-Scraping Samples
Code samples for web scraping

## Introduction

There are two methods to scrape web pages

1. Submit HTTP request to a web server and download the HTML source file. Then parse the HTML file for the desired information.
2. Control a (headless) web browser application to navigate and render web sites. Then query DOM elements to extract the desired information.

The first method is fairly straight forward. You can use the Python packagesrequestsand bs4(BeautifulSoup) to query the HTML document and then parse through the HTML tags of the web page. This method works well for static web pages, or dynamic pages that are created entirely on the server side. However, this method fails for content that is build up from JavaScript code in the browser.

The second method uses an actual web browser like Google Chrome to load and render pages. JavaScript code will be executed in the same way the as if the page would have been opened manually. The Selenium driver allows your code to control user actions such as clicking buttons or links, and even inserting text.
In contrast to the first method, you have to query the tree of DOM objects. https://developer.mozilla.org/en-US/docs/Web/API/Document_Object_Model/Introduction

## Example

Simple example code that runs on ARC

```python
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import os
chrome_options = Options()
chrome_options.add_argument("--headless")

chrome_options.add_argument("--window-size=1920x1080")
chrome_driver = '/usr/lib64/chromium-browser/chromedriver'

driver = webdriver.Chrome(options=chrome_options, executable_path=chrome_driver)


# driver.get("http://google.com")
driver.get("http://arc.insight.gsu.edu")

# process DOM, extract information, take a screen shot
driver.save_screenshot('capture.png')

# make sure to terminate the browser process
driver.close()  # Don’t forget to call these commands when you’re done
driver.quit()   # Otherwise the Chrome Browser application keeps running and taking up major resources
```

## References

Here are just a few links. Both scraping methods are commonly used, and there is plenty of documentation on the Internet. You will also find code snippets. Please, check the age of example code and the versions of the Python packages that were used. Unfortunately, APIs change over the years.

1. Scraping static or server-side created pages:
    1. https://www.statworx.com/at/blog/web-scraping-101-in-python-with-requests-beautifulsoup/
    1. https://www.dataquest.io/blog/web-scraping-python-using-beautiful-soup/

2. Scraping all types of pages (including dynamic client-side):
    1. https://towardsdatascience.com/web-scraping-using-selenium-python-8a60f4cf40ab
    1. https://medium.com/@pyzzled/running-headless-chrome-with-selenium-in-python-3f42d1f5ff1d
    1. Installation of selenium https://pypi.org/project/selenium/
    1. Selenium documentation https://www.selenium.dev/documentation/

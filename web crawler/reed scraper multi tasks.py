import requests
import time
from lxml import etree
import csv
import schedule
import gevent
from gevent import monkey
from gevent.queue import Queue

# create a environment of coroutine of tasks
monkey.patch_all()
# create a queue for muti-tasks
queue = Queue()
page_end= int(input('please enter number of pages:'))
# add every single url into queue
for pagenumber in range(1, page_end):
    url = 'https://www.reed.co.uk/jobs/data-analyst-jobs-in-england?pageno=?' + str(pagenumber)
    queue.put_nowait(url)
headers= {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}

def job_crawler():
    file = open('data_analyst.csv','a')
    writer = csv.writer(file)
    url = queue.get_nowait()  #extract url from queue
    response = requests.get(url=url, headers=headers)
    response.encoding = 'utf-8'
    # turn resonse to element
    root = etree.HTML(response.content)
    url = root.xpath('//article[@class="job-result   "]/a/@href')
    for i in url:
        link = 'https://www.reed.co.uk/' + i
        # print(link)
        # time.sleep(1)
        job_contents = requests.get(url=link, headers=headers)
        # job_contents.encoding ='utf-8'
        jobs = etree.HTML(job_contents.content)
        # print(jobs)
        descriptions = jobs.xpath('//span[@itemprop ="description"]/descendant::text()')
        salary = jobs.xpath('//span[@data-qa="salaryMobileLbl"]/text()')
        location = jobs.xpath('//span[contains(@itemprop,"addressLocality") and @data-qa ="regionLbl"]/text()')
        job_type = jobs.xpath('//span[contains(@itemprop,"employmentType") and @data-qa="jobTypeLbl"]/text()')
        job_title = jobs.xpath('string(//meta[contains(@itemprop,"title")]/@content)')
        post_date = jobs.xpath('string(//meta[contains(@itemprop,"datePosted")]/@content)')
        # print(job_title, post_date, salary, location, job_type, descriptions,link)
        # ws.append([job_title, post_date, salary, location, job_type, descriptions,link])
        writer.writerow([job_title, post_date, salary, location, job_type, descriptions,link])
        print('adding data .......')

            # 'job_title', 'post date', 'salary or wages', 'location', 'job type', 'job description')
    print('Finished!!!!')
    file.close()
    return file


tasks=[]
for i in range(1,page_end):   # use gevent.spawn() create gevent object and run spider at multitasks
    task = gevent.spawn(job_crawler)
    tasks.append(task)
gevent.joinall(tasks) #execute multi tasks at same time
# automate scrapping reed.co.uk to update job infor
# schedule.every().friday.at('10:30').do(job_crawler)
#
# while True:
#     schedule.run_pending()
#     time.sleep(1)
#!/usr/bin/python3

import os
import pdfplumber
import jieba
import threading
import multiprocessing

from concurrent import futures
from collections import Counter
from datetime import datetime

filePath = "./"

keyWords = "key"

# 并发数量 = cpu核数   
# 目前使用cpu核数会报错，环境为虚拟机ubuntu
# max_workers = multiprocessing.cpu_count()
# 手动配置一下并发数量吧
max_workers = 4
print("当前并发数量：", max_workers)

# 锁
main_thread_lock = threading.Lock()

def getPdfText(fileName):
    # 获取pdf中的文本
    content = ""
    with pdfplumber.open(fileName) as pdf:
        for page in pdf.pages:
            content += page.extract_text()
    return content


def handleSingle(fileName):
    # 处理单个pdf
    kv = {}
    content = getPdfText(os.path.join(filePath, fileName))
    res = jieba.lcut_for_search(content)
    for word in res:
        if keyWords in word:
            if kv.get(word) is None:
                kv[word] = 0
            kv[word] += 1

    return fileName, kv


def filterNotPdf():
    # 过滤非pdf文件
    fileNames = os.listdir(filePath)
    notPdfFileNames = []
    pdfFileNames = []
    for fileName in fileNames:
        if fileName.endswith(".pdf"):
            pdfFileNames.append(fileName)
            continue

        notPdfFileNames.append(fileName)

    print("非pdf文件：", notPdfFileNames)
    return pdfFileNames


if __name__ == "__main__":
    start = datetime.now()
    pdfFileNames = filterNotPdf()

    all_kv = {}

    # 读取pdf文件内容
    with futures.ProcessPoolExecutor(max_workers=max_workers) as pool:
        for fn, kv in pool.map(handleSingle, pdfFileNames):
            try:
                with main_thread_lock:
                    X, Y = Counter(kv), Counter(all_kv)
                    all_kv = dict(X+Y)

                    print("-" * 100)
                    print("fn:\t", kv)
                    print("all: \t", all_kv)
            except Exception as e:
                print(e)
                
    
    end = datetime.now()
    # calc waste time
    wasteTime = (end - start).seconds
    print("wasteTime:", wasteTime, "秒")

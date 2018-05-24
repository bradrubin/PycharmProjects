import os

from bs4 import BeautifulSoup

websiteDir = "/Users/bsrubin/Website/NPRoadTrip/"
indexTrailer = open(websiteDir + "National Parks.html")
soup = BeautifulSoup(indexTrailer, 'html5lib')
indexTrailer.close()

entryHeader = '''<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01//EN"> \
                 <html><head><meta charset='UTF-8'> \
                 <link rel='stylesheet' type='text/css' href='day-one.css' /> \
                 <link rel='stylesheet' type='text/css' href='day-one-html-override.css' /> \
                 </head><body id='body' dir='auto'>'''
entryTrailer = '''<script src='javascript.js'></script></body></html>'''

entries = soup.findAll("div", {"class": "entry"})
for entry in entries:
    contents = entryHeader + ''.join(map(lambda t: str(t), entry.contents[1:])) + entryTrailer
    fileName = entry.contents[4].getText() + ".html"
    file = open(websiteDir + fileName, "w")
    file.write(contents)
    file.close()


def getEntries(path):
    entries = []
    if len(path) > 0:
        entries = [fileName for fileName in os.listdir(path) if os.path.isfile(path + fileName)]
        entries = [fileName for fileName in entries if fileName.endswith('.html') and
                         fileName != "Erin & Kellen's Wedding.html" and
                         fileName != "National Parks.html" and
                         fileName != "index.html"]
    return entries


fileNames = getEntries(websiteDir)
indexHeader = '''<!DOCTYPE html><html><body><h1 align="center">Brad & Deb's National Parks Road Trip</h1><ol> \
                 <CENTER><IMG SRC="NPRT.png" ALIGN="BOTTOM"> </CENTER><HR>'''
indexTrailer = '</ol></body></html>'

for fileName in fileNames:
    indexHeader = indexHeader + ('<li><a href="' + fileName + '">' + fileName[:-5] + "</a></li>")
indexHeader = indexHeader + indexTrailer
index = open(websiteDir + "index.html", "w")
index.write(indexHeader)
index.close()

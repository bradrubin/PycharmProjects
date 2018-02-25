import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta
from PIL import Image
from hdfs.ext.kerberos import KerberosClient
from io import BytesIO
from mpl_toolkits.basemap import Basemap
from pyproj import Proj
from pyspark import SparkContext, SQLContext
from subprocess import check_call
from sys import argv


def addMap(outDir, image, satLongitude, xmin, xmax, ymin, ymax, dt):
    plt.switch_backend('agg')
    plt.figure(figsize=(25, 15), dpi=100)
    m = Basemap(projection='geos', lon_0=satLongitude,
                resolution='i', area_thresh=1000,
                llcrnrx=xmin, llcrnry=ymin,
                urcrnrx=xmax, urcrnry=ymax)
    m.imshow(np.flipud(image[1]))
    m.drawcoastlines()
    m.drawcountries()
    m.drawstates()
    # plt.title('GOES-16 Pseudo Color\n%s' % dt.strftime('%B %d, %Y UTC'))
    buf = BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', pad_inches=0)
    buf.seek(0)
    client = KerberosClient('http://hc.gps.stthomas.edu:50070')
    with client.write(outDir + '/MAP_' + image[0].split("/")[-1], overwrite=True) as writer:
        writer.write(buf.getvalue())
    buf.close()


def transform(outDir, image, x, y, dt):
    plt.switch_backend('agg')
    plt.figure(figsize=(25, 15), dpi=100)
    p = Proj(proj='geos', h=satHeight, lon_0=satLongitude, sweep=satSweep)
    XX, YY = np.meshgrid(x, y)
    lons, lats = p(XX, YY, inverse=True)
    mH = Basemap(resolution='i', projection='lcc', area_thresh=1500,
                 width=1800 * 3000, height=1060 * 3000,
                 lat_1=38.5, lat_2=38.5,
                 lat_0=38.5, lon_0=-97.5)
    xH, yH = mH(lons, lats)
    rgb = image[1][:, :-1, :]
    rgb = rgb / 256.0
    colorTuple = rgb.reshape((rgb.shape[0] * rgb.shape[1]), 3)
    colorTuple = np.insert(colorTuple, 3, 1.0, axis=1)
    newmap = mH.pcolormesh(xH, yH, image[1][:, :, 0], color=colorTuple, linewidth=0)
    newmap.set_array(None)
    mH.drawstates()
    mH.drawcountries()
    mH.drawcoastlines()
    # plt.title('GOES-16 Pseudo Color\n%s' % dt.strftime('%B %d, %Y UTC'))
    buf = BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', pad_inches=0)
    buf.seek(0)
    client = KerberosClient('http://hc.gps.stthomas.edu:50070')
    with client.write(outDir + '/TRANSFORM_' + image[0].split("/")[-1], overwrite=True) as writer:
        writer.write(buf.getvalue())
    buf.close()


sc = SparkContext(appName="color")
sqlContext = SQLContext(sc)
inputDir = argv[1]
outputDir = argv[2]
numPartitions = int(argv[3])
df = sqlContext.read.parquet(inputDir + '/satMetadata.parquet')
first = df.first()
satHeight = first[2]
x = list(map(lambda x: x * satHeight, first[0]))
xmin = min(x)
xmax = max(x)
y = list(map(lambda x: x * satHeight, first[1]))
ymin = min(y)
ymax = max(y)
satLongitude = first[3]
satSweep = first[4]
date = first[5]
add_seconds = date
displayDate = datetime(2000, 1, 1, 12) + timedelta(seconds=add_seconds)
images = sc.binaryFiles(inputDir + '/*.png', numPartitions)
imageToArray = lambda rawdata: np.asarray(Image.open(BytesIO(rawdata))).astype(np.uint8)
imageArrays = images.mapValues(imageToArray)
imageArrays.foreachPartition( lambda x: check_call(["kinit", "-kt", "brad.keytab", "brad@GPS.STTHOMAS.EDU"] ))
imageArrays.map(lambda image: addMap(outputDir, image, satLongitude, xmin, xmax, ymin, ymax, displayDate)).collect()
imageArrays.map(lambda image: transform(outputDir, image, x, y, displayDate)).collect()

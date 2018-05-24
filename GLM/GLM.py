import matplotlib.animation as animation
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
from netCDF4 import Dataset

dataset = Dataset(
    '/Users/bsrubin/Desktop/GLM-L2-LCFA-2018-064-00-OR_GLM-L2-LCFA_G16_s20180640000000_e20180640000200_c20180640000224.nc')

lat = dataset.variables['flash_lat'][:]
lon = dataset.variables['flash_lon'][:]
count = int(dataset.variables['flash_count'][0])
satLon = dataset.variables['nominal_satellite_subpoint_lon'][0]

plt.figure(figsize=[20, 20])
m = Basemap(projection='geos', lon_0=satLon, resolution='i', area_thresh=10000)
transformedLon, transformedLat = m(lon, lat)
point = m.plot([], [], 'yo', markersize=4)[0]

m.bluemarble()
m.drawcoastlines()
m.drawcountries()
m.drawstates()

# Intial blank map drawing
def init():
    point.set_data([], [])
    return point,


# Called for each frame, with incremented index value. After the first 10 points are displayed, drop the least
# recently used point as each new point is added to the map.
def animate(index):
    start = 0
    if index > 10:
        start = index - 10
    point.set_data(transformedLon[start:index], transformedLat[start:index])
    return point,


anim = animation.FuncAnimation(plt.gcf(), animate, init_func=init, frames=count, interval=50, blit=True)
anim.save('GLM.mp4', codec="libx264", extra_args=['-pix_fmt', 'yuv420p'], writer="ffmpeg")
# HTML(anim.to_html5_video())

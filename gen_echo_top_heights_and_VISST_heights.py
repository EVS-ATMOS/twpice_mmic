import matplotlib
matplotlib.use('Agg')
import pyart
from netCDF4 import Dataset
import numpy as np
from datetime import datetime, timedelta
from copy import deepcopy
import glob
import math
import dask.array as da
import time
import sys
from scipy import interpolate
import time_procedures

# Input the range of dates and time wanted for the collection of images
start_year = 2006
start_day = 20
start_month = 1
start_hour = 0
start_minute = 1
start_second = 0

end_year = 2006
end_month = 1
end_day = 20
end_hour = 3
end_minute = 1
end_second = 0

data_path = '/lcrc/group/earthscience/rjackson/multidop_grids/ddop/'
visst_data_path = '/lcrc/group/earthscience/rjackson/visst/'

# get_radar_times
#     start_year = Start year of animation
#     start_month = Start month of animation
#     start_day = Start day of animation
#     start_hour = Start hour of animation
#     end_year = End year of animation
#     end_month = End month of animation
#     end_day = End day of animation
#     end_minute = End minute of animation
#     minute_interval = Interval in minutes between scans (default is 5)
# This procedure acquires an array of Radar classes between start_time and end_time  
def get_dda_times(start_year, start_month, start_day,
                  start_hour, start_minute, end_year,
                  end_month, end_day, end_hour, 
                  end_minute, minute_interval=1):

    start_time = datetime(start_year,
                      start_month,
                      start_day,
                      start_hour,
                      start_minute,
                      )
    end_time = datetime(end_year,
                      end_month,
                      end_day,
                      end_hour,
                      end_minute,
                      )

    deltatime = end_time - start_time

    if(deltatime.seconds > 0 or deltatime.minute > 0):
        no_days = deltatime.days + 1
    else:
        no_days = deltatime.days
    
    if(start_day != end_day):
        no_days = no_days + 1
        
    days = np.arange(0, no_days, 1)
    print('We are about to load grid files for ' + str(no_days) + ' days')
    
    # Find the list of files for each day
    cur_time = start_time
 
    file_list = []
    time_list = []
    for i in days:
        year_str = "%04d" % cur_time.year
        day_str = "%02d" % cur_time.day
        month_str = "%02d" % cur_time.month
        format_str = (data_path +
                      'cf_compliant_grid' +
                      year_str +
                      month_str +
                      day_str +
                      '*.nc')
    
        print('Looking for files with format ' + format_str)
          
        data_list = glob.glob(format_str)
        
        for j in range(0, len(data_list)):
            file_list.append(data_list[j])
        cur_time = cur_time + timedelta(days=1)
    
    # Parse all of the dates and time in the interval and add them to the time list
    past_time = []
    for file_name in file_list:
        date_str = file_name[-15:-3]
        year_str = date_str[0:4]
        month_str = date_str[4:6]
        day_str = date_str[6:8]
        hour_str = date_str[8:10]
        minute_str = date_str[10:12]
        second_str = '00'
             
        cur_time = datetime(int(year_str),
                            int(month_str),
                            int(day_str),
                            int(hour_str),
                            int(minute_str),
                            0)
        time_list.append(cur_time)
        
    
    # Sort time list and make sure time are at least xx min apart
    time_list.sort()
    time_list_sorted = deepcopy(time_list)
   
    time_list_final = []
    past_time = []
    
    for times in time_list_sorted: 
        
        cur_time = times  
        
        if(past_time == []):
            past_time = cur_time
            
        if(cur_time - past_time >= timedelta(minutes=minute_interval)
           and cur_time >= start_time and cur_time <= end_time): 
            time_list_final.append(cur_time)
            past_time = cur_time
           
    return time_list_final

def get_visst_times(start_year, start_month, start_day,
                    start_hour, start_minute, end_year,
                    end_month, end_day, end_hour, 
                    end_minute):
    
    start_time = datetime(start_year,
                      start_month,
                      start_day,
                      start_hour,
                      start_minute,
                      )
    end_time = datetime(end_year,
                      end_month,
                      end_day,
                      end_hour,
                      end_minute,
                      )
    deltatime = end_time - start_time
    if(deltatime.seconds > 0 or deltatime.minute > 0):
        no_days = deltatime.days + 1
    else:
        no_days = deltatime.days

    days = np.arange(0, no_days, 1)
    # Find the list of files for each day
    cur_time = start_time

    file_list = []
    time_list = []
    for i in days:
        year_str = "%04d" % cur_time.year
        day_str = "%02d" % cur_time.day
        month_str = "%02d" % cur_time.month
        print('Looking for files with format ' +
              visst_data_path +
              'twpvisstpx04*' +
              year_str +
              month_str +
              day_str +
              '*.cdf')
        data_list = glob.glob(visst_data_path +
                              'twpvisstpx04*' +
                              year_str +
                              month_str +
                              day_str +
                              '*.cdf')
        if(data_list):
            if(cur_time >= start_time and cur_time <= end_time):
                file_list.append(data_list[0])  
                time_list.append(cur_time)
        cur_time = cur_time + timedelta(days=1)
    return file_list, time_list

def get_visst_from_time(cur_time):
    year_str = "%04d" % cur_time.year
    day_str = "%02d" % cur_time.day
    month_str = "%02d" % cur_time.month
    data_list = glob.glob(visst_data_path +
                          'twpvisstpx04*' +
                          year_str +
                          month_str +
                          day_str +
                          '*.cdf')
    if(data_list):
        return Dataset(data_list[0])
    else:
        return []

# Get a Radar object given a time period in the CPOL dataset
def get_grid_from_dda(time):
    year_str = "%04d" % time.year
    month_str = "%02d" % time.month
    day_str = "%02d" % time.day
    hour_str = "%02d" % time.hour
    minute_str = "%02d" % time.minute
    second_str = "%02d" % time.second
    file_name_str = (data_path +
                    'cf_compliant_grid' +
                     year_str +
                     month_str +
                     day_str +
                     hour_str +
                     minute_str + '.nc')
    
    radar = pyart.io.read_grid(file_name_str)
    return radar

def dms_to_decimal(deg, minutes, seconds):
    return deg+minutes/60+seconds/3600

# Convert seconds to midnight to a string format
def seconds_to_midnight_to_string(time_secs_after_midnight):
    hours = math.floor(time_secs_after_midnight/3600)
    minutes = math.floor((time_secs_after_midnight - hours*3600)/60)
    temp = datetime.time(int(hours), int(minutes), )
    return temp.strftime('%H%M%S')

def seconds_to_midnight_to_hm(time_secs_after_midnight):
    hours = math.floor(time_secs_after_midnight/3600)
    minutes = math.floor((time_secs_after_midnight - hours*3600)/60)
    return hours, minutes

def get_echotop_heights(cur_time):
    # First, get VISST Tb 
    cdf_data = get_visst_from_time(cur_time)

    # Load lat, lon, and time parameters - try statement for 24-hourly data, except for daily data
    Latitude = cdf_data.variables['latitude'][:]
    Longitude = cdf_data.variables['longitude'][:]
    Time = cdf_data.variables['image_times'][:]
    NumPixels = cdf_data.variables['image_numpix'][:]

    # Load brightness temperature
    IRBrightness = cdf_data.variables['temperature_ir'][:]
    CloudTopHeight = cdf_data.variables['cloud_top_height'][:]
    num_frames = len(NumPixels)
    
    echo_top_temps_visst = []
    echo_top_temps_cpol = []
    # For each time, find multidop grid that is within 10 minutes of scan
    for frame in range(0, num_frames):
        scan_hr, scan_min = seconds_to_midnight_to_hm(Time[frame])
        five_minutes_before = datetime(cur_time.year,
                                       cur_time.month,
                                       cur_time.day,
                                       int(scan_hr), 
                                       int(scan_min)) - timedelta(minutes=5)
        five_minutes_after = datetime(cur_time.year,
                                      cur_time.month,
                                      cur_time.day,
                                      int(scan_hr), 
                                      int(scan_min)) + timedelta(minutes=5)

        nearest_multidop = get_dda_times(five_minutes_before.year,
                                         five_minutes_before.month,
                                         five_minutes_before.day,
                                         five_minutes_before.hour,
                                         five_minutes_before.minute,
                                         five_minutes_after.year,
                                         five_minutes_after.month,
                                         five_minutes_after.day,
                                         five_minutes_after.hour,
                                         five_minutes_after.minute)
        print(cur_time.year, cur_time.month, cur_time.day, scan_hr, scan_min)
        print(nearest_multidop)
        resolution = 5
        if(len(nearest_multidop) > 0):
            try:
                pyart_grid = time_procedures.get_grid_from_cpol(nearest_multidop[0])
            except:
                print('Py-ART grid not found!')
                continue
            texture = pyart_grid.fields['velocity_texture']['data']
            z = pyart_grid.fields['DT']['data']
            grid_z = pyart_grid.point_z['data']
            # Get sounding data
            one_day_ago = nearest_multidop[0]-timedelta(days=1, minutes=1)
            sounding_times = time_procedures.get_sounding_times(one_day_ago.year,
                                                                one_day_ago.month,
                                                                one_day_ago.day,
                                                                one_day_ago.hour,
                                                                one_day_ago.minute,
                                                                cur_time.year,
                                                                cur_time.month,
                                                                cur_time.day,
                                                                cur_time.hour,
                                                                cur_time.minute,
                                                                minute_interval=60)
            try:
                sounding_time = sounding_times[len(sounding_times)-1]
                Sounding_netcdf = time_procedures.get_sounding(sounding_time)
                base_time = Sounding_netcdf.variables['base_time'][:]
                alt = Sounding_netcdf.variables['alt'][:]
                temp = Sounding_netcdf.variables['tdry'][:]
                Tz = interpolate.interp1d(alt, temp+273.15)
                grid_temp = Tz(grid_z)
            except:
                print('Insufficient information from sounding...skipping!')
                continue

            array_shape = texture.shape
            # Get echo top heights from CPOL
            echo_top = np.zeros((array_shape[1],array_shape[2]))
            for i in range(0, array_shape[1]):
                for j in range(0, array_shape[2]):
                    in_cloud = np.where(texture[:,i,j] < 3)
                    if(len(in_cloud[0]) > 0):
                        in_cloud = in_cloud[0][-1]
                        echo_top[i,j] = grid_z[in_cloud,i,j]/1e3
                    else:
                        echo_top[i,j] = np.nan
            
            
            # Get brightness temperatures from VISST over same grid
            # Load brightness temperature           
            cpol_latitude = -12.249166
            cpol_longitude = 131.04445
            
            # Get Lat and Lon for specific frame
            Lat = Latitude[(int(frame)*int(NumPixels[frame])):(int(frame+1)*int(NumPixels[frame])-1)]
            Lon = Longitude[(int(frame)*int(NumPixels[frame])):(int(frame+1)*int(NumPixels[frame])-1)]
            
            Lon_cpol = pyart_grid.point_longitude['data'][0]
            Lat_cpol = pyart_grid.point_latitude['data'][0]
            Lon_cpol = Lon_cpol.flatten()
            Lat_cpol = Lat_cpol.flatten()
            # Regrid data to multidop's grid
            x = pyart_grid.point_longitude['data'][0,
                                                   ::resolution,
                                                   ::resolution]
            y = pyart_grid.point_latitude['data'][0,
                                                  ::resolution,
                                                  ::resolution]
            echo_top = interpolate.griddata((Lon_cpol, Lat_cpol),
                                            echo_top.flatten(),
                                            (x,y))
            echo_top_temps_cpol.append(echo_top)                                            
            data = CloudTopHeight[(int(frame)*int(NumPixels[frame])):(int(frame+1)*int(NumPixels[frame])-1)]
            data_gridded = interpolate.griddata((Lon,Lat), data, (x,y))
            lat_gridded = interpolate.griddata((Lon,Lat), Lat, (x,y))
            lon_gridded = interpolate.griddata((Lon,Lat), Lon, (x,y))
            lat_bounds = np.logical_or(lat_gridded > cpol_latitude+1.5,
                                       lat_gridded < cpol_latitude-1.5)
            lon_bounds = np.logical_or(lon_gridded < cpol_longitude-1.5,
                                       lon_gridded > cpol_longitude+1.5)
            masked_region = np.logical_or(lat_bounds, lon_bounds)
            data_masked = np.ma.array(data_gridded)
            data_masked = np.ma.masked_where(masked_region, data_gridded)
            echo_top_temps_visst.append(data_masked)

    if(echo_top_temps_visst != []):
        dims = echo_top.shape
        return_array = np.zeros((2,len(echo_top_temps_visst), dims[0], dims[1]))
        echo_top_temps_visst = np.stack(echo_top_temps_visst)
        echo_top_temps_cpol = np.stack(echo_top_temps_cpol)
        return_array[0,:,:,:] = echo_top_temps_cpol
        return_array[1,:,:,:] = echo_top_temps_visst
        return return_array
    else:
        return []


# Get the multidop grid times
files, times = get_visst_times(start_year, start_month, start_day,
                               start_hour, start_minute, end_year,
                               end_month, end_day, end_hour, 
                               end_minute)

# Calculate PDF
num_levels = 1
print('Doing parallel grid loading...')
import time
t1 = time.time()
tbs = []
num_times = len(times)
hours = []
minutes = []
seconds = []
years = []
days = []
months = []
for cur_time in times:
    tbs_temp = get_echotop_heights(cur_time)
    if(len(tbs_temp) > 0):
        tbs.append(tbs_temp)
        tbs_shape = tbs_temp.shape
        years.append(cur_time.year*np.ones(tbs_shape[1]))
        days.append(cur_time.day*np.ones(tbs_shape[1]))
        months.append(cur_time.month*np.ones(tbs_shape[1]))
        hours.append(cur_time.hour*np.ones(tbs_shape[1]))
        minutes.append(cur_time.minute*np.ones(tbs_shape[1]))
        seconds.append(cur_time.second*np.ones(tbs_shape[1]))

for arrays in tbs:
    print(arrays.shape)

years = np.concatenate([arrays for arrays in years])
days = np.concatenate([arrays for arrays in days])
months = np.concatenate([arrays for arrays in months])
hours = np.concatenate([arrays for arrays in hours])
minutes = np.concatenate([arrays for arrays in minutes])
seconds = np.concatenate([arrays for arrays in seconds])
tbs = np.concatenate([arrays for arrays in tbs], axis=1)
print(years.shape)
t2 = time.time() - t1
print('Total time in s: ' + str(t2))
print('Time per scan = ' + str(t2/len(times)))           
print('Writing netCDF file...')

# Save to netCDF file
out_netcdf = Dataset('echo_top_heights_Jan20.cdf', 'w')
array_dims = tbs.shape
print(array_dims)
out_netcdf.createDimension('time', array_dims[1])
out_netcdf.createDimension('x_len', array_dims[2])
out_netcdf.createDimension('y_len', array_dims[3])
cpol_file = out_netcdf.createVariable('cpol_T_echo_top', tbs[0].dtype, ('time', 'x_len', 'y_len'))
cpol_file.long_name = 'Temp @ echo top height from CPOL'
cpol_file.units = 'km'
cpol_file[:] = tbs[0]

visst_file = out_netcdf.createVariable('visst_Tv', tbs[0].dtype, ('time', 'x_len', 'y_len'))
visst_file.long_name = 'visst echo top height'
visst_file.units = 'km'
visst_file[:] = tbs[1]

years_file = out_netcdf.createVariable('year', years.dtype, ('time'))
years_file.long_name = 'Year'
years_file.units = 'YYYY'
years_file[:] = years

months_file = out_netcdf.createVariable('month', years.dtype, ('time'))
months_file.long_name = 'Month'
months_file.units = 'MM'
months_file[:] = months

hours_file = out_netcdf.createVariable('hour', hours.dtype, ('time'))
hours_file.long_name = 'Hour'
hours_file.units = 'HH'
hours_file[:] = hours

days_file = out_netcdf.createVariable('day', days.dtype, ('time'))
days_file.long_name = 'Day'
days_file.units = 'DD'
days_file[:] = days

minutes_file = out_netcdf.createVariable('minute', minutes.dtype, ('time'))
minutes_file.long_name = 'Minute'
minutes_file.units = 'HH'
minutes_file[:] = minutes

seconds_file = out_netcdf.createVariable('second', seconds.dtype, ('time'))
seconds_file.long_name = 'Second'
seconds_file.units = 'SS'
seconds_file[:] = seconds
                                      
out_netcdf.close()

# Import variables
import pyart
import numpy as np
from netCDF4 import Dataset

wrf_file = '/home/rjackson/data/wrftestdata/wrfout_d01_2006-01-20_000000'
out_path = '/home/rjackson/data/'

# Get a Radar object given a time period in the CPOL dataset
def convert_wrf_to_grid(wrf_dataset, time_step):    
    # The pregridded files are not loadable using read_grid, 
    # so load in the necessary fields and make the Grid object out of them
    FillValue = -32768
    Z_wrf1 = wrf_dataset.variables['REFL_10CM'][time_step]
    Lat_wrf = wrf_dataset.variables['XLAT'][time_step]
    Lon_wrf = wrf_dataset.variables['XLONG'][time_step]
    W_wrf = wrf_dataset.variables['W'][time_step]
    PH_wrf = wrf_dataset.variables['PH'][time_step]
    PHB_wrf = wrf_dataset.variables['PHB'][time_step]
    time_str = wrf_dataset.variables['Times'][time_step]
    year_str = time_str[0] + time_str[1] + time_str[2] + time_str[3]
    month_str = time_str[5] + time_str[6]
    day_str = time_str[8] + time_str[9]
    hour_str = time_str[11] + time_str[12]
    minute_str = time_str[14] + time_str[15]
    second_str = time_str[17] + time_str[18]
    
    alt_wrf = np.zeros(PH_wrf.shape)
    array_shape = PH_wrf.shape
    Z_wrf = FillValue*np.ones(array_shape)
    alt_wrf = (PH_wrf+PHB_wrf)/9.81
    #Z_wrf = -32768*np.ones(alt_wrf.shape)
    Z_wrf[1:] = Z_wrf1
    Z_wrf = np.ma.masked_where(Z_wrf == FillValue, Z_wrf)
    # Add grid locations
        
    H_mean = np.zeros(array_shape[0])
    for i in range(0, array_shape[0]-1):
        H_mean[i] = np.nanmean(alt_wrf[i,:,:])
    grid_x = {'data': np.arange(-array_shape[2]/2,array_shape[2]/2,1)*1e3}
    grid_y = {'data': np.arange(-array_shape[2]/2,array_shape[1]/2,1)*1e3}
    levs = {'data': H_mean}    
    
    # Place into dictionary
    W_wrf = {'data': W_wrf,
             'long_name': 'updraft_velocity',
             'units': 'm/s',
             '_FillValue': FillValue}
    
    Z_wrf = {'data': Z_wrf,
             'long_name': 'reflectivity',
             'units': 'dBZ',
             '_FillValue': FillValue}
    
    # Lat, lon same everywhere
    cpol_location = [Lat_wrf[1,1], Lon_wrf[1,1]]
    print(cpol_location)
         
    # Add grid fields
    cpol_grid_fields = {'W': W_wrf,
                        'reflectivity': Z_wrf}
    
    metadata = {}
    
    
    # Add latitude and longitude entries for each radar
    origin_latitude = {'data': [cpol_location[0]],
                       'units': 'degrees'}
    origin_longitude = {'data': [cpol_location[1]],
                        'units': 'degrees'}
    cpol_latitude = {'data': [cpol_location[0]],
                       'units': 'degrees'}
    cpol_longitude = {'data': [cpol_location[1]],
                        'units': 'degrees'}
    origin_altitude = {'data': [50.],
                       'units': 'meters'}
    cpol_altitude = {'data': [50.],
                       'units': 'meters'}
    cpol_name = {'data': 'W'} 
    time_dict = {'units': ('seconds since ' + 
                           year_str + 
                           '-' + 
                           month_str + 
                           '-' + 
                           day_str + 
                           'T' + 
                           hour_str + 
                           ':' + 
                           minute_str + 
                           ':' +
                           second_str + 'Z'), 
                 'long_name': 'Time of grid', 'standard_name': 
                 'time', 'data': np.array([0.], dtype=float), 
                 'calendar': 'gregorian'}
    print(time_dict['units'])
    
    # Create grid objects
    grid_wrf = pyart.core.Grid(time_dict, 
                               cpol_grid_fields,
                               metadata,
                               origin_latitude,
                               origin_longitude,
                               origin_altitude,
                               grid_x, 
                               grid_y,
                               levs, 
                               radar_latitude=cpol_latitude,
                               radar_longitude=cpol_longitude,
                               radar_altitude=cpol_altitude,
                               radar_name=cpol_name,
                               radar_time=time_dict)
    return grid_wrf

wrf_cdf = Dataset(wrf_file, mode='r')
 
for time_indicies in range(0,18):
    wrf_grid = convert_wrf_to_grid(wrf_cdf, time_indicies)
    conv_strat = pyart.retrieve.steiner_conv_strat(wrf_grid)
    # Write convective-stratiform classification into netCDF file
    fname = (out_path +
             'conv_strat_wrf' +
             str(time_indicies) +
             '.nc') 
    fname_grid = (out_path + 'wrf_' + str(time_indicies) + '.nc')
    pyart.io.write_grid(fname_grid, wrf_grid)

    out_netcdf = Dataset(fname, mode='w')            
    out_netcdf.createDimension('X', len(wrf_grid.x['data']))
    out_netcdf.createDimension('Y', len(wrf_grid.y['data']))

    x_file = out_netcdf.createVariable('Grid_X', 'f4', ('X',))
    x_file.long_name = 'Distance north of center'
    x_file.units = 'km'
    x_file[:] = wrf_grid.x['data']/1e3
        
    y_file = out_netcdf.createVariable('Grid_Y', 'f4', ('Y',))
    y_file.long_name = 'Distance east of center'
    y_file.units = 'km'
    y_file[:] = wrf_grid.y['data']/1e3
         
    conv_file = out_netcdf.createVariable('strat_conv', 'i4', ('X','Y'))
    conv_file.long_name = 'Steiner convective classification'
    conv_file.units = '0 = Little precip, 1 = Stratiform, 2 = Convective'
    conv_file[:] = conv_strat['data']
    out_netcdf.close()


import geopandas as gpd
import matplotlib.pyplt as plt

def main():
    '''
    Edit a geodataframe, save as a shapefile
    '''

    shp_fn = 'F:\UrbanDataProject\seattle_hackathon\data\senior_service_contracts.shp'

    #import data
    sf = gpd.read_file(shp_fn)

    # add columns
    vals = sf['TotalBudgt']

    # write shapefile
    plt.hist(vals, )
    plt.xlabel('Investment')
    plt.ylabel('Number of Service Providers')

if __name__ == '__main__':
    main()
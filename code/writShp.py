import geopandas as gpd

def main():
    '''
    Edit a geodataframe, save as a shapefile
    '''

    shp_fn = '../data/other_services/sea_hospitals.shp'

    #import data
    sf = gpd.read_file(shp_fn)

    # add columns
    sf['Project'] = 'Hospital'
    sf['TotalBudgt'] = 1
    sf['LineofBiz'] = 'Hospital'
    sf['ContractNo'] = sf.index

    # write shapefile
    sf.to_file(driver = 'ESRI Shapefile', filename= "../data/other_services/sea_hosp_edit.shp")

if __name__ == '__main__':
    main()
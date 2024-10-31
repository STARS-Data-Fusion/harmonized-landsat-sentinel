import earthaccess

def get_CMR_granule_ID(granule: earthaccess.search.DataGranule):
    return granule["meta"]["native-id"]

from importlib import resources


# data from https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Electric_Holding_Company_Areas/FeatureServer/0/query\?where\=NAME%20like%20%27%25UNS%20ELECTRIC%25%27\&outSR\=3857\&f\=geojson # NOQA
# with the appropriate name query
TEP = resources.read_text(__name__, 'tep.json')
APS = resources.read_text(__name__, 'aps.json')
PNM = resources.read_text(__name__, 'pnm.json')

import pkg_resources


def load_geoip():
    import geoip2.database

    resource_package = __name__
    filename = pkg_resources.resource_filename(resource_package, 'GeoLite2-Country.mmdb')
    return geoip2.database.Reader(filename)

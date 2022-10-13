import os
import setuptools

setuptools.setup(
  name='dataflow-snippets',
  version='0.0.1',
  install_requires=['geoip2'],
  packages=setuptools.find_packages(),
  package_data={
   'resources': ['GeoLite2-Country.mmdb'],     # All files from folder A
   },
)

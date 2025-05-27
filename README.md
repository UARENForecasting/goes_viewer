# goes_viewer
Displays data from goes-18 satellite over the Arizona/New Mexico areas.
Can be accessed at https://forecasting.energy.arizona.edu/satellite_viewer/

### Running locally
> python figure.py

will generate a static index.html, but cannot access images in the /figs/ directory.


> goes-viewer process-files noaa-goes19 ABI-L2-MCMIPC /\<this directory>/figs/ -v

will pull files from AWS and save them as png's

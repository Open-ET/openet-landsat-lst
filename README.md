# Landsat Thermal Band Sharpening Algorithms

The thermal sharpening algorithm is developed for Landsat 5, 7, and 8. The algorithm has three components.  

* **Global process**: first, a random forest model is built using spatially homogeneous samples selected from the whole Landsat scene. To achieve this, the shortwave spectral bands (30m) are first aggregated to match the resolution of the TIR band. Pixels with low spatial heterogeneity are selected to build a random forest regressor between shortwave surface reflectance and TIR brightness temperature.  
* **Local process**: second, local models are built for each aggregated pixel using a moving window (neighbor analysis) with a predefined size. Homogeneous pixels within the window are used to establish simple linear regression between shortwave surface reflectance and TIR brightness temperature.  
* **Residual redistribution**: finally, both local and global results are aggregated to and compared to the TIR band. The final model is computed as a linear combination of the local and global results using the reversed squared residual as weights. 
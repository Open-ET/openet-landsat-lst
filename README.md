# Landsat LST Sharpening Algorithms

The thermal sharpening algorithm is developed for Landsat 5, 7, 8, or 9. The algorithm has three components.  

* **Global process**: first, a random forest model is built using spatially homogeneous samples selected from the whole Landsat scene. To achieve this, the shortwave spectral bands (30m) are first aggregated to match the resolution of the TIR band. Pixels with low spatial heterogeneity are selected to build a random forest regressor between shortwave surface reflectance and TIR brightness temperature.  
* **Local process**: second, local models are built for each aggregated pixel using a moving window (neighbor analysis) with a predefined size. Homogeneous pixels within the window are used to establish simple linear regression between shortwave surface reflectance and TIR brightness temperature.  
* **Combination**: thirdly, both local and global results are aggregated to initial TIR resolution and compared to the TIR band. The difference between aggregated sharpened TIR and original TIR is computed as residuals. The combined model is a linear combination of the local and global results using the reversed squared residual as weights. 
* **Energy conservation**: finally, the combined sharpened image is aggregated to TIR resolution and residuals are computed. To avoid box-like features, the residual image is resampled to higher resolution with bilinear interpolation and added back to the combined sharpened image.

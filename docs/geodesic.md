# Geodesic Coordinates

For simple use of geodesic coordinates we offer a basic functionality for transfromation into a cartesian coordinate frame (UTM).


To use, add this configuration block:

```
cartesian_transform:  
    type: "utm"
    # utm_zone: 33
    # utm_band: "N"
    lon: 16.511422682732736
    lat: 47.977274686327114
    inplace: true # directly transform utm to map
    broadcast_cartesian_transform: false
    cartesian_frame_id: "utm"
    world_frame_id: "map"
```


!!! note 
    Set `geodesic=True` on each query that contains geodesic coordinates

## Parameters

* `type` specify type of cartesian coordinate. Supported: `[utm]`
* `utm_zone`/`utm_band` directly set utm zone and band
* `lat`/`lon` used to auto detect utm zone if not directly set 
* `inplace`: sets `lon`/`lat` as origin of our local map frame and directly transform points into this frame
* `broadcast_cartesian_transform`: broadcast the transformation between `[cartesian_frame_id]-->[world_frame_id]`
* `cartesian_frame_id` name of cartesian frame
* `world_frame_id` name of local map frame
  

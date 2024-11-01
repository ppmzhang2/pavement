"""Visualising a WKT polygons in a map using Folium."""

import argparse
import os
from collections.abc import Callable

import folium
import geopandas as gpd
import pandas as pd
from shapely import wkt

TH_LVL4 = 10.0
TH_LVL3 = 8.0
TH_LVL2 = 4.5

# Default Colors
# - red: "#EA2924"
# - orange: "#F1A72B"
# - green: "#1E7C14"
COLOR_LVL4 = "#C70039"  # dark-red-like
COLOR_LVL3 = "#FF5733"  # pink-like
COLOR_LVL2 = "#F1A72B"  # orange
COLOR_LVL1 = "#1E7C14"  # green

html_legend = f"""
<div style="
position: fixed; 
bottom: 50px; left: 5px; width: 150px; height: 120px; 
z-index:9999; font-size:14px;
background-color: transparent;
">
<h4 style="margin:5px; text-align:left;">
  Condition
</h4>
<ul style="list-style:none; padding:2px; margin:0px;">
  <li>
    <span style="background-color:{COLOR_LVL1}; color: {COLOR_LVL1};
    margin-right: 5px; padding: 5px;">
      &#9632;
    </span> Good
  </li>
  <li>
    <span style="background-color:{COLOR_LVL2}; color:{COLOR_LVL2};
    margin-right: 5px; padding: 5px;">
      &#9632;
    </span> Fair
  </li>
  <li>
    <span style="background-color:{COLOR_LVL3}; color:{COLOR_LVL3};
    margin-right: 5px; padding: 5px;">
      &#9632;
    </span> Poor
  </li>
  <li>
    <span style="background-color:{COLOR_LVL4}; color:{COLOR_LVL4};
    margin-right: 5px; padding: 5px;">
      &#9632;
    </span> Very Poor
  </li>
</ul>
</div>
"""


def _func_style(color: str) -> Callable:
    return lambda _: {
        "fillColor": color,
        "color": color,
        "weight": 2,
        "dashArray": "5, 5",
    }


def main(tsv: str, output_dir: str) -> None:
    """Visualising a WKT polygons in a map using Folium."""
    data = pd.read_csv(tsv, sep="\t")

    feat_uuid = data["feature_id"].to_list()
    roads = data["road"].to_list()
    geoms = data["wkt"].to_list()
    ratios = data["ratio"].to_list()
    m = folium.Map(
        location=[-43.5321, 172.6362],
        tiles="Cartodb Positron",
        width="100%",
        height="100%",
        control_scale=True,
        zoom_start=13,
        max_zoom=21,
    )

    for ind, geom in enumerate(geoms):
        # Convert WKT to shapely polygon
        shapely_geom = wkt.loads(geom)
        # Create a GeoDataFrame
        gdf = gpd.GeoDataFrame(
            index=[0],
            crs="EPSG:2193",
            geometry=[shapely_geom],
        )
        gdf = gdf.to_crs("EPSG:4326")
        geojson_data = gdf.to_json()  # Convert GeoDataFrame to GeoJSON
        # Add the GeoJSON data to the map
        c = (COLOR_LVL4
             if ratios[ind] > TH_LVL4 else COLOR_LVL3 if ratios[ind] > TH_LVL3
             else COLOR_LVL2 if ratios[ind] > TH_LVL2 else COLOR_LVL1)

        folium.GeoJson(
            geojson_data,
            style_function=_func_style(c),
            popup=folium.map.Popup(
                html=f'<a href="{feat_uuid[ind]}.html">{roads[ind]}</a>',
                parse_html=False,
                lazy=True,
            ),
            tooltip=f"{roads[ind]}",
        ).add_to(m)

    # Save the map to an HTML file
    m.get_root().html.add_child(folium.Element(html_legend))
    m.save(os.path.join(output_dir, "index.html"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Visualising a WKT polygons in a map using Folium.")
    parser.add_argument(
        "-i",
        "--input",
        type=str,
        required=True,
        help="Input TSV file containing the WKT polygons.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        required=True,
        help="Output directory to save the map.",
    )
    args = parser.parse_args()
    main(args.input, args.output)

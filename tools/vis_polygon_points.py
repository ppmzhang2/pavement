"""Visualising a WKT polygon with a set of points."""

import argparse
import os
from collections.abc import Callable

import folium
import geopandas as gpd
import pandas as pd
from shapely import wkt

TH_RED = 800
TH_ORANGE = 400

TH_LVL4 = 10.0
TH_LVL3 = 8.0
TH_LVL2 = 4.0

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
        "fillColor": "none",
        "color": color,
        "weight": 2,
        "dashArray": "5, 5",
    }


def main(tsv: str, out_dir: str, *, predict: bool = True) -> None:
    """Visualising a WKT polygon with a set of points."""
    data_all = pd.read_csv(tsv, sep="\t")
    feature_seq_max = data_all["feature_seq"].max().item()

    for feature_idx in range(1, feature_seq_max + 1):
        data = data_all[data_all["feature_seq"] == feature_idx]

        feat_uuid = data["feature_id"].to_list()[0]
        geom = data["wkt"].to_list()[0]
        ratio = data["ratio"].to_list()[0]
        dates = data["prefix"].to_list()
        fault_files = data["fault_image"].to_list()
        latitudes = data["latitude"].to_list()
        longitudes = data["longitude"].to_list()
        coordinates = list(zip(latitudes, longitudes, strict=True))
        color_polygon = (COLOR_LVL4
                         if ratio > TH_LVL4 else COLOR_LVL3 if ratio > TH_LVL3
                         else COLOR_LVL2 if ratio > TH_LVL2 else COLOR_LVL1)
        color_point = [
            COLOR_LVL4 if "_verypoor" in name else COLOR_LVL3 if "_poor"
            in name else COLOR_LVL2 if "_fair" in name else COLOR_LVL1
            for name in fault_files
        ]

        # find the center of the coordinates
        center_lat = sum(latitudes) / len(latitudes)
        center_lon = sum(longitudes) / len(longitudes)

        # Convert WKT to shapely polygon
        shapely_polygon = wkt.loads(geom)

        # Create a GeoDataFrame
        gdf = gpd.GeoDataFrame(index=[0],
                               crs="EPSG:2193",
                               geometry=[shapely_polygon])
        gdf = gdf.to_crs("EPSG:4326")
        geojson_data = gdf.to_json()  # Convert GeoDataFrame to GeoJSON

        m = folium.Map(
            location=[center_lat, center_lon],
            tiles="Cartodb Positron",
            width="100%",
            height="100%",
            control_scale=True,
            zoom_start=19,
            max_zoom=21,
        )

        # Add the GeoJSON data to the map
        folium.GeoJson(geojson_data,
                       style_function=_func_style(color_polygon)).add_to(m)

        for ind, coord in enumerate(coordinates):
            dir_root = ("images" if color_point[ind] == COLOR_LVL1 else
                        "pred" if predict else "distress")
            html = (f'<a href="https://fdt.mzsys.eu.org/{dir_root}/'
                    f'{dates[ind]}/{fault_files[ind]}">{fault_files[ind]}</a>')

            folium.CircleMarker(
                location=coord[:2],
                color=color_point[ind],
                radius=1,  # Adjust the size of the circles
                weight=3,
                opacity=0.7,
                fill=False,
                interactive=False,
                popup=folium.map.Popup(
                    html=html,
                    parse_html=False,
                    lazy=True,
                ),
                tooltip=fault_files[ind],
                # fill_color="blue",  # Adjust the fill color of the circles
            ).add_to(m)

        m.get_root().html.add_child(folium.Element(html_legend))
        # Save the map to an HTML file
        m.save(os.path.join(out_dir, f"{feat_uuid}.html"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Visualising a WKT polygon "
                                     "with a set of points.")
    parser.add_argument(
        "-i",
        "--input",
        type=str,
        required=True,
        help="Input TSV file containing the WKT polygon and points.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        required=True,
        help="Output directory to save the HTML files.",
    )
    parser.add_argument(
        "--no-predict",
        action="store_true",
        help="Set this flag to disable the prediction mode.",
    )
    args = parser.parse_args()
    main(args.input, args.output, predict=not args.no_predict)

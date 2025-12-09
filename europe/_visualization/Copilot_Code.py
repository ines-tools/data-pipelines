import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib as mpl
from matplotlib.patches import Wedge
from matplotlib.colors import Normalize, LogNorm
import matplotlib.cm as cm
import cartopy.crs as ccrs

from shapely.geometry import box
import pyproj
from shapely.ops import transform

import cartopy.feature as cfeature
import pyproj


def points_to_data_radius(ax, x_data, y_data, r_pt, sample_len_data=100000.0):
    """
    Convert a desired radius in points to a data-space radius at (x_data, y_data),
    using the current axes transform. Works best when the axes data units are metric
    (e.g., LAEA meters). For LAEA Europe, sample_len_data=100000 (100 km) is fine.

    Parameters
    ----------
    ax : matplotlib.axes.Axes (Cartopy GeoAxes)
    x_data, y_data : float
        Center in *axes data coordinates* (i.e., projection units, meters for LAEA).
    r_pt : float
        Desired radius in points (screen units).
    sample_len_data : float
        Small data step used to estimate local data->pixel scaling (meters).

    Returns
    -------
    r_data : float
        Radius in data units so that the rendered radius is ~r_pt on screen.
    """
    # points -> pixels
    r_px = r_pt * (ax.figure.dpi / 72.0)

    # Transform the center to pixel coords
    p0 = ax.transData.transform((x_data, y_data))

    # Sample X and Y scaling (data step -> pixel step)
    p_x = ax.transData.transform((x_data + sample_len_data, y_data))
    p_y = ax.transData.transform((x_data, y_data + sample_len_data))

    px_per_step_x = abs(p_x[0] - p0[0])
    px_per_step_y = abs(p_y[1] - p0[1])

    # Guard against degenerate scales
    if px_per_step_x <= 1e-9 or px_per_step_y <= 1e-9:
        return sample_len_data  # fallback, won't happen in normal projections

    # meters per pixel in X and Y
    m_per_px_x = sample_len_data / px_per_step_x
    m_per_px_y = sample_len_data / px_per_step_y

    # Use geometric mean to get an isotropic radius in data units
    m_per_px_iso = np.sqrt(m_per_px_x * m_per_px_y)

    return r_px * m_per_px_iso

def plot_node_mix_choropleth_wedges(
    gdf_countries, df_cap, flows, node, scalar, colorbar_title, year,
    gdf_id='id',           # column in gdf with country codes ('ES','FR','PT',…)
    df_country='polygon',  # column in df with the same country codes
    tech_col='technology',
    bubble_frac=0.08,      # pie radius as fraction of map width (0.06–0.12 works well)
    min_slice_pct=0.01,    # fold tiny wedges into 'Other'
    cmap='viridis',        # choropleth colormap for totals
    use_log=False,         # log color scale if totals are skewed
    label_offset_pts=(9, 0),
    target_epsg=3035,      # reproject to EPSG:3035 (ETRS89 / LAEA Europe)
    savepath=None
):
    # ---------- 0) Reproject countries to 3035 and keep only countries in CSV ----------
    if gdf_countries.crs is None:
        raise ValueError("gdf_countries has no CRS. Set a CRS first (e.g., EPSG:4326).")
    g = gdf_countries.to_crs(target_epsg).copy()

    # ---------- 1) Filter to node and aggregate ----------
    df_node = df_cap[df_cap['node'] == node].copy()
    if df_node.empty:
        raise ValueError(f"No rows for node='{node}'.")
    totals = (df_node.groupby(df_country)[year]
                     .sum()
                     .rename('capacity')
                     .reset_index())

    # Keep only overlap countries
    g_sub = g.merge(totals, left_on=gdf_id, right_on=df_country, how='inner')
    if g_sub.empty:
        raise ValueError("No overlap between country IDs in gdf and CSV.")

    # ---------- 2) Tech mix per country (fold small to 'Other') ----------
    d = (df_node.groupby([df_country, tech_col])[year].sum().reset_index())
    d = d.merge(totals[[df_country, 'capacity']].rename(columns={'capacity':'total'}),on=df_country, how='left')
    d['pct'] = np.where(d['total'] > 0, d[year] / d['total'], 0)
    d.loc[d['pct'] < min_slice_pct, tech_col] = "Other"
    pies = (d.groupby([df_country, tech_col])[year].sum().reset_index())

    techs  = sorted(pies[tech_col].unique().tolist())
    colors = {t: plt.cm.tab20(i % 20) for i, t in enumerate(techs)}
    order  = techs[:]  # stable order

    
    # Also get lat/lon coordinates for pie placement on the map
    g_latlon = g_sub.to_crs(4326).copy()
    reps_ll = g_latlon.geometry.representative_point()
    g_latlon['cx_ll'], g_latlon['cy_ll'] = reps_ll.x, reps_ll.y


    # ---------- 3) Plot choropleth (totals) ----------
    fig = plt.figure(figsize=(10, 7))
    ax = plt.axes(projection=ccrs.LambertAzimuthalEqualArea(central_longitude=10, central_latitude=50))

    ax.set_title(f"Portfolio Mix - {year} – {node}",
                 loc='left', fontsize=12, weight='bold')

    
    ax.add_feature(cfeature.COASTLINE)
    gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    gl.top_labels = False
    gl.right_labels = False  
    gl.x_inline = False
    gl.y_inline = False

    
    # Convert capacity to GW for display
    g_sub = g_sub.copy()
    g_sub['capacity'] = g_sub['capacity'] / scalar
    g_sub["area_m2"] = g_sub.geometry.area
    area_lookup = dict(zip(g_sub[gdf_id].values, g_sub["area_m2"].values))
    
    minA = float(np.nanmin(g_sub["area_m2"].values))
    maxA = float(np.nanmax(g_sub["area_m2"].values))

    # Choropleth
    g_latlon_plot = g_sub.to_crs(4326)
    g_latlon_plot.plot(column='capacity', cmap=cmap, ax=ax, transform=ccrs.PlateCarree(),
                  edgecolor='#777', linewidth=0.7)

    
    # Normalization (log or linear)
    if use_log:
        # Ensure vmin strictly positive for LogNorm
        vmin = max(1e-6, float(np.nanmin(np.where(g_sub['capacity']>0, g_sub['capacity'], np.nan))))
        norm = LogNorm(vmin=vmin, vmax=float(g_sub['capacity'].max()))
    else:
        norm = Normalize(vmin=float(g_sub['capacity'].min()), vmax=float(g_sub['capacity'].max()))

    cmap_obj = mpl.colormaps.get_cmap(cmap)

    # Plot polygons in lat/lon coordinates
    g_latlon_plot = g_sub.to_crs(4326)
    for _, row in g_latlon_plot.iterrows():
        face = cmap_obj(norm(row['capacity'])) if np.isfinite(row['capacity']) else "#f0f0f0"
        ax.add_geometries([row.geometry], crs=ccrs.PlateCarree(),
                          facecolor=face, edgecolor='#777', linewidth=0.7)
    
    # Colorbar
    sm = cm.ScalarMappable(norm=norm, cmap=cmap_obj)
    sm.set_array([])
    cb = fig.colorbar(sm, ax=ax, orientation="vertical", shrink=0.65, pad=0.05)
    cb.set_label(colorbar_title, fontsize=12)

    # Auto-zoom to data bounds (lat/lon)
    xmin, ymin, xmax, ymax = g_latlon_plot.total_bounds
    dx, dy = xmax - xmin, ymax - ymin
    pad_x = 0.2 * dx 
    pad_y = 0.04 * dy 
    ax.set_extent([xmin - pad_x, xmax + pad_x, ymin - pad_y, ymax + pad_y],crs=ccrs.PlateCarree())
    
    # Show axis frame and grid (visual box edge) 
    for spine in ax.spines.values():
        spine.set_edgecolor('#333')
        spine.set_linewidth(1.2)
    
    
    # Compute radius in data units (map degrees) for better scaling
    r_deg = bubble_frac * dx

    # Draw wedges in geographic coordinates but as screen circles
    for idx, row in g_latlon.iterrows():
        cid = row[gdf_id]
        lon, lat = row['cx_ll'], row['cy_ll']

        A = area_lookup.get(cid, np.nan)
        if np.isnan(A) or A <= 0:
            continue
        k = 0.35
        r_m = k * np.sqrt(A)

        s = pies[pies[df_country] == cid].set_index(tech_col)[year].reindex(order).fillna(0.0)
        tot = float(s.sum())
        if tot <= 0:
            continue

        # Transform geographic center to screen coordinates
        x_screen, y_screen = ax.projection.transform_point(lon, lat, ccrs.PlateCarree())

        start = 90.0
        for tech in order:
            val = float(s.loc[tech])
            if val <= 0:
                continue
            sweep = 360.0 * val / tot

            wedge = Wedge((x_screen, y_screen), r_m, start, start + sweep,
                        facecolor=colors[tech], edgecolor='white', linewidth=0.6,
                        transform=ax.projection)
            wedge.set_zorder(6)
            ax.add_patch(wedge)

            start += sweep

        # Country code label
        ax.text(lon+0.5, lat+0.5, str(cid), fontsize=10, weight='bold',
                ha='center', va='center', color='#111',
                transform=ccrs.PlateCarree(), zorder=7)


    # Keep only pairs present in the country layer
    flows = flows[flows["source"].isin(g_sub.id.values) & flows["target"].isin(g_sub.id.values) & (flows["commodity"] == node)]
    
    node_colors = {"electricity": "#d95f02","hydrogen": "#1b9e77","methane": "#7570b3","hydrocarbons":"#e7298a"}
    line_color = node_colors.get(node, "#333")

    if not flows.empty:
        # Scale linewidth by value (normalize to [lw_min, lw_max])
        v = flows[flows.commodity == node][year].astype(float).values
        if len(v) > 0: 
            vmax = np.nanmax(v)
            lw_min, lw_max = 5.0, 20.0  # adjust as you like

            
            flows = flows.copy()
            flows["lw"] = lw_min + (lw_max - lw_min) * (flows[year].astype(float) / vmax)

            # Draw lines; put them under wedges but above the basemap
            for _, rflow in flows.iterrows():
                # Get lat/lon from merged dataframe
                sx, sy = g_latlon.loc[g_latlon[gdf_id] == rflow["source"], ["cx_ll", "cy_ll"]].values[0]
                tx, ty = g_latlon.loc[g_latlon[gdf_id] == rflow["target"], ["cx_ll", "cy_ll"]].values[0]
                
                ax.plot([sx, tx], [sy, ty],
                        transform=ccrs.PlateCarree(),
                        color=line_color, linewidth=float(rflow["lw"]),
                        alpha=0.9, zorder=4)

                        
            # Compute percentiles
            ref_values = [round(flows[year].astype(float).max()*0.2,0), round(flows[year].astype(float).max()*0.8,0)]
            ref_labels = [f"{v:.0f} TWh" for v in ref_values]
            ref_widths = lw_min + (lw_max - lw_min) * (np.array(ref_values) / vmax)
          
            # Legend anchor in screen coordinates (top-left area)
            y_cursor = 0.92  # screen y (1.0 = top)
            x_start = 0.05   # screen x (0 = left)
            
            for lbl, w in zip(ref_labels, ref_widths):
                # Sample line in screen space
                ax.plot([x_start, x_start + 0.15], [y_cursor, y_cursor],
                        transform=ax.transAxes,
                        color=line_color, linewidth=w*0.3, alpha=0.9)
                # Label
                ax.text(x_start + 0.2, y_cursor, lbl, fontsize=11, va="center", ha="left",
                        transform=ax.transAxes, color="#111")
                # Spacing
                y_cursor -= 0.04

    
    # --- Technology legend: box with edges at bottom-right corner ---
    handles = [plt.Line2D([0],[0], marker='o', color='none',
                        markerfacecolor=colors[t], markersize=10, label=t)
            for t in order]
    ax.legend(handles=handles, loc='lower right',
            ncol=1, frameon=True, fontsize=9, fancybox=False, edgecolor='#333',
            title="Technology", title_fontsize=10)

    # Gridline labels: avoid bottom labels to prevent overlap with legend
    gl.bottom_labels = False
    gl.top_labels = True

    # Leave room at the bottom for legend (and on the right for colorbar)
    plt.subplots_adjust(left=0.01, right=0.99, top=0.93, bottom=0.02)
    
    if savepath:
        plt.savefig(f"{savepath}_{node}.png", dpi=600)

    return fig, ax


def main():

    
    gdf = gpd.read_file("onshore_PECD1.geojson")  # must have column 'id'
    df = pd.read_csv("files_out/unit_to_flows.csv",index_col=0)
    df_f = pd.read_csv("files_out/crossborder_flows.csv",index_col=0)

    for node in df["node"].unique():
        print(f"Doing node {node}")
        plot_node_mix_choropleth_wedges(
            gdf_countries=gdf,
            df_cap=df,
            flows = df_f,
            node=node,
            scalar=1e6,
            colorbar_title = "Total Energy Produce (TWh)",
            year="y2050",
            gdf_id="id",
            df_country="polygon",
            tech_col="technology",
            bubble_frac=0.19,   # make wedges larger or smaller here
            cmap="viridis",
            use_log=False,      # True if distribution is very skewed
            target_epsg=3035,
            savepath="maps/energy_mix"
        )

if __name__ == "__main__":
    main()
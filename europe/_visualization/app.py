import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import geopandas as gpd
import dill
import numpy as np

st.set_page_config(page_title="Capacity Dashboard", layout="wide")

# ----------------------
# Load CSV files
# ----------------------
def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, index_col=0)
    df = df.loc[:, ~df.columns.str.contains(r"^Unnamed")]
    return df

installed = load_csv("files_out/installed_capacity.csv")
invested = load_csv("files_out/invested_capacity.csv")
decommissioned = load_csv("files_out/decommissioned_capacity.csv")
unit_to_flows = load_csv("files_out/unit_to_flows.csv")

# ----------------------
# Helper: Melt wide to long
# ----------------------
ID_VARS = ["unit_name", "node", "scenario", "polygon", "technology"]

def melt_df(df: pd.DataFrame, value_name: str) -> pd.DataFrame:
    year_cols = [c for c in df.columns if c.startswith("y") and c[1:].isdigit()]
    return df.melt(id_vars=ID_VARS, value_vars=year_cols, var_name="year", value_name=value_name)

installed_m = melt_df(installed, "Installed")
invested_m = melt_df(invested, "Invested")
decom_m = melt_df(decommissioned, "Decommissioned")
unit_flows_m = melt_df(unit_to_flows, "UnitFlows")

# Merge all
merged = installed_m.merge(invested_m, on=ID_VARS + ["year"], how="outer").merge(
    decom_m, on=ID_VARS + ["year"], how="outer"
).merge(unit_flows_m, on=ID_VARS + ["year"], how="outer")

# Extract numeric year and clean
merged["year"] = merged["year"].str.extract(r"(\d+)")
merged = merged.dropna(subset=["year"]).copy()
merged["year"] = merged["year"].astype(int)

# Clean categories
for col in ["technology", "polygon", "node", "scenario"]:
    merged[col] = merged[col].astype(str).str.strip()
merged["technology"] = merged["technology"].replace({"nan": "Unknown"})

# Convertir de MW a GW
merged["Installed"] = merged["Installed"] / 1000
merged["Invested"] = merged["Invested"] / 1000
merged["Decommissioned"] = merged["Decommissioned"] / 1000
merged["UnitFlows"] = merged["UnitFlows"] / 1e6

# ----------------------
# Sidebar filters
# ----------------------
st.sidebar.header("Global Filters")
scenarios = sorted(merged["scenario"].dropna().unique())
countries = sorted(merged["polygon"].dropna().unique())
nodes = sorted(merged["node"].dropna().unique())
years = sorted(merged["year"].dropna().unique())

scenario = st.sidebar.selectbox("Scenario", scenarios)
selected_countries = st.sidebar.multiselect("Countries", ["Europe"] + countries, default=countries)
selected_node = st.sidebar.selectbox("Node", ["All"] + nodes)
year_option = st.sidebar.selectbox("Year", ["All Years"] + [str(y) for y in years])

# Filter data
if "Europe" in selected_countries:
    filtered = merged[merged["scenario"] == scenario].copy()
    selected_countries_display = ["Europe"]
else:
    filtered = merged[(merged["scenario"] == scenario) & (merged["polygon"].isin(selected_countries))].copy()
    selected_countries_display = selected_countries

if selected_node != "All":
    filtered = filtered[filtered["node"] == selected_node]
if year_option != "All Years":
    filtered = filtered[filtered["year"] == int(year_option)]

# ----------------------
# Color palette
# ----------------------
def assign_color_by_technology(tech_name):
    """Asigna colores basados en el tipo de tecnología"""
    tech_lower = tech_name.lower()
    
    if 'solar' in tech_lower or 'pv' in tech_lower or 'photovoltaic' in tech_lower:
        return '#FFD700'
    elif 'wind' in tech_lower or 'eolica' in tech_lower or 'eólica' in tech_lower:
        return '#87CEEB'
    elif 'hydro' in tech_lower or 'hydra' in tech_lower or 'water' in tech_lower or 'dam' in tech_lower:
        return '#1E90FF'
    elif 'nuclear' in tech_lower or 'uranium' in tech_lower:
        return '#9370DB'
    elif 'gas' in tech_lower and 'biogas' not in tech_lower:
        return '#A9A9A9'
    elif 'coal' in tech_lower or 'carbon' in tech_lower or 'carbón' in tech_lower:
        return '#2F4F4F'
    elif 'oil' in tech_lower or 'diesel' in tech_lower or 'fuel' in tech_lower:
        return '#8B4513'
    elif 'biomass' in tech_lower or 'bio' in tech_lower or 'waste' in tech_lower:
        return '#6B8E23'
    elif 'geothermal' in tech_lower or 'geo' in tech_lower:
        return '#FF6347'
    elif 'battery' in tech_lower or 'storage' in tech_lower or 'bess' in tech_lower:
        return '#32CD32'
    elif 'renewable' in tech_lower or 'green' in tech_lower or 'clean' in tech_lower:
        return '#00FA9A'
    elif 'turbine' in tech_lower:
        return '#B0C4DE'
    elif 'ccgt' in tech_lower or 'combined cycle' in tech_lower:
        return '#C0C0C0'
    elif 'chp' in tech_lower or 'cogeneration' in tech_lower or 'cogen' in tech_lower:
        return '#DAA520'
    else:
        import hashlib
        hash_val = int(hashlib.md5(tech_name.encode()).hexdigest(), 16)
        r = (hash_val % 155) + 100
        g = ((hash_val >> 8) % 155) + 100
        b = ((hash_val >> 16) % 155) + 100
        return f'#{r:02x}{g:02x}{b:02x}'

TECH_ORDER = sorted(merged["technology"].dropna().unique())
color_map = {tech: assign_color_by_technology(tech) for tech in TECH_ORDER}

if "Europe" not in selected_countries:
    order_polygons = (filtered.groupby("polygon")["Installed"]
                      .sum()
                      .sort_values(ascending=False)
                      .index.tolist()) if not filtered.empty else selected_countries_display
else:
    order_polygons = []

facet_rows = filtered["year"].nunique() if (len(selected_countries_display) > 1 and year_option == "All Years") else 1
height_value = 400
fig_height = max(height_value, height_value * facet_rows)

# ----------------------
# Plot 1: Installed Capacity
# ----------------------
st.subheader("Installed Capacity")

if filtered.empty:
    st.info("No data for the selected filters.")
else:
    group_cols = ["technology"]
    if year_option == "All Years":
        group_cols.append("year")
    if len(selected_countries_display) > 1 and "Europe" not in selected_countries:
        group_cols.append("polygon")
    if selected_node == "All":
        group_cols.append("node")

    agg_installed = filtered.groupby(group_cols, as_index=False)["Installed"].sum().rename(columns={"Installed":"Capacity (GW)"})
    agg_installed = agg_installed[agg_installed["Capacity (GW)"].fillna(0) > 0]

    years = sorted(agg_installed["year"].dropna().unique())  # e.g., [2030, 2040, 2050]
    YEAR_ORDER = list(map(int, years))

    if agg_installed.empty:
        st.info("No installed capacity data for the selected filters.")
    else:
        if (len(selected_countries_display) == 1 or "Europe" in selected_countries) and year_option != "All Years" and selected_node != "All":
            agg_installed["year"] = year_option
            fig_installed = px.bar(
                agg_installed, x="year", y="Capacity (GW)", color="technology",
                color_discrete_map=color_map, category_orders={"technology": TECH_ORDER},
                title=f"Installed Capacity in {', '.join(selected_countries_display)} - {selected_node} - {year_option} ({scenario})"
            )
        elif year_option == "All Years":
            if len(selected_countries_display) > 1 and "Europe" not in selected_countries:
                fig_installed = px.bar(
                    agg_installed, x="polygon", y="Capacity (GW)", color="technology",
                    color_discrete_map=color_map, barmode="stack", facet_row="year",
                    category_orders={"polygon": order_polygons, "technology": TECH_ORDER, "year": YEAR_ORDER},
                    title=f"Installed Capacity by Country per Year ({scenario})"
                )
            else:
                fig_installed = px.bar(
                    agg_installed, x="year", y="Capacity (GW)", color="technology",
                    color_discrete_map=color_map, barmode="stack",
                    category_orders={"technology": TECH_ORDER},
                    title=f"Installed Capacity in {', '.join(selected_countries_display)} ({scenario})"
                )
        else:
            if len(selected_countries_display) > 1 and "Europe" not in selected_countries:
                fig_installed = px.bar(
                    agg_installed, x="polygon", y="Capacity (GW)", color="technology",
                    color_discrete_map=color_map, barmode="stack",
                    category_orders={"polygon": order_polygons, "technology": TECH_ORDER},
                    title=f"Installed Capacity in {year_option} by Country ({scenario})"
                )
            else:
                if selected_node == "All":
                    fig_installed = px.bar(
                        agg_installed, x="node", y="Capacity (GW)", color="technology",
                        color_discrete_map=color_map, barmode="stack",
                        category_orders={"technology": TECH_ORDER},
                        title=f"Installed Capacity in {', '.join(selected_countries_display)} - {year_option} by Node ({scenario})"
                    )
                else:
                    fig_installed = px.bar(
                        agg_installed, x=["Total"], y="Capacity (GW)", color="technology",
                        color_discrete_map=color_map, category_orders={"technology": TECH_ORDER},
                        title=f"Installed Capacity in {', '.join(selected_countries_display)} - {selected_node} - {year_option} ({scenario})"
                    )

        fig_installed.update_layout(
            height=fig_height, bargap=0.35, template="plotly_white",
            legend_title_text="Technology")
        st.plotly_chart(fig_installed, width="stretch")

# ----------------------
# Plot 2: Unit to Flows
# ----------------------
st.subheader("Unit to Flows")

if filtered.empty:
    st.info("No data for the selected filters.")
else:
    group_cols = ["technology"]
    if year_option == "All Years":
        group_cols.append("year")
    if len(selected_countries_display) > 1 and "Europe" not in selected_countries:
        group_cols.append("polygon")
    if selected_node == "All":
        group_cols.append("node")

    agg_flows = filtered.groupby(group_cols, as_index=False)["UnitFlows"].sum().rename(columns={"UnitFlows":"Flows (TWh)"})
    agg_flows = agg_flows[agg_flows["Flows (TWh)"].fillna(0) > 0]

    
    years = sorted(agg_flows["year"].dropna().unique())  # e.g., [2030, 2040, 2050]
    YEAR_ORDER = list(map(int, years))


    if agg_flows.empty:
        st.info("No unit to flows data for the selected filters.")
    else:
        if (len(selected_countries_display) == 1 or "Europe" in selected_countries) and year_option != "All Years" and selected_node != "All":
            agg_flows["year"] = year_option
            fig_flows = px.bar(
                agg_flows, x="year", y="Flows (TWh)", color="technology",
                color_discrete_map=color_map, category_orders={"technology": TECH_ORDER},
                title=f"Unit to Flows in {', '.join(selected_countries_display)} - {selected_node} - {year_option} ({scenario})"
            )
        elif year_option == "All Years":
            if len(selected_countries_display) > 1 and "Europe" not in selected_countries:
                fig_flows = px.bar(
                    agg_flows, x="polygon", y="Flows (TWh)", color="technology",
                    color_discrete_map=color_map, barmode="stack", facet_row="year",
                    category_orders={"polygon": order_polygons, "technology": TECH_ORDER, "year": YEAR_ORDER},
                    title=f"Unit to Flows by Country per Year ({scenario})"
                )
            else:
                fig_flows = px.bar(
                    agg_flows, x="year", y="Flows (TWh)", color="technology",
                    color_discrete_map=color_map, barmode="stack",
                    category_orders={"technology": TECH_ORDER},
                    title=f"Unit to Flows in {', '.join(selected_countries_display)} ({scenario})"
                )
        else:
            if len(selected_countries_display) > 1 and "Europe" not in selected_countries:
                fig_flows = px.bar(
                    agg_flows, x="polygon", y="Flows (TWh)", color="technology",
                    color_discrete_map=color_map, barmode="stack",
                    category_orders={"polygon": order_polygons, "technology": TECH_ORDER},
                    title=f"Unit to Flows in {year_option} by Country ({scenario})"
                )
            else:
                if selected_node == "All":
                    fig_flows = px.bar(
                        agg_flows, x="node", y="Flows (TWh)", color="technology",
                        color_discrete_map=color_map, barmode="stack",
                        category_orders={"technology": TECH_ORDER},
                        title=f"Unit to Flows in {', '.join(selected_countries_display)} - {year_option} by Node ({scenario})"
                    )
                else:
                    fig_flows = px.bar(
                        agg_flows, x=["Total"], y="Flows (TWh)", color="technology",
                        color_discrete_map=color_map, category_orders={"technology": TECH_ORDER},
                        title=f"Unit to Flows in {', '.join(selected_countries_display)} - {selected_node} - {year_option} ({scenario})"
                    )

        fig_flows.update_layout(
            height=fig_height, bargap=0.35, template="plotly_white",
            legend_title_text="Technology")
        st.plotly_chart(fig_flows, width="stretch")

# ----------------------
# Plot 3: Invested vs Decommissioned
# ----------------------

st.subheader("Invested vs Decommissioned (by Technology)")

fig_height = 600
if filtered.empty:
    st.info("No data for the selected filters.")
else:
    # ===== NUEVO: lógica para "Europe" =====
    if "Europe" in selected_countries:
        agg = filtered.groupby(["technology", "year", "node"], as_index=False)[["Invested", "Decommissioned"]].sum()
    else:
        agg = filtered.groupby(["technology", "year", "polygon", "node"], as_index=False)[["Invested", "Decommissioned"]].sum()

    invested_data = agg[agg["Invested"].fillna(0) > 0].copy()
    invested_data["Value"] = invested_data["Invested"]

    decom_data = agg[agg["Decommissioned"].fillna(0) > 0].copy()
    decom_data["Value"] = -decom_data["Decommissioned"]

    multiple_plots = None

    # ===== Ajustar x_col según selección =====
    if "Europe" in selected_countries:
        if year_option == "All Years":
            x_col = "year"
        else:
            # ===== NUEVO: si Europe + un año + todos los nodos → agregamos por tecnología =====
            if selected_node == "All":
                x_col = "technology"
                invested_data = invested_data.groupby(["technology"], as_index=False)["Value"].sum()
                decom_data = decom_data.groupby(["technology"], as_index=False)["Value"].sum()
            else:
                x_col = "node"
    else:
        x_col = "year"
        if year_option == "All Years" and len(selected_countries) > 1:
            x_col = "polygon"
            multiple_plots = "year"
        elif year_option != "All Years" and len(selected_countries) > 1:
            x_col = "polygon"
        elif selected_node == "All" and len(selected_countries) == 1:
            # Un país + todos los nodos → agregamos por año
            x_col = "year"
            invested_data = invested_data.groupby(["technology", "year"], as_index=False)["Value"].sum()
            decom_data = decom_data.groupby(["technology", "year"], as_index=False)["Value"].sum()
        elif len(selected_countries) == 1:
            x_col = "year"

    # ===== PLOTEO =====
    if multiple_plots:
        years_list = sorted(agg["year"].unique())
        for year in years_list:
            inv_year = invested_data[invested_data["year"] == year]
            dec_year = decom_data[decom_data["year"] == year]
            
            fig_change = go.Figure()
            x_order = sorted(inv_year[x_col].dropna().unique())

            for tech in TECH_ORDER:
                inv_tech = inv_year[inv_year["technology"] == tech]
                dec_tech = dec_year[dec_year["technology"] == tech]
                
               
                has_inv = not inv_tech.empty
                has_dec = not dec_tech.empty

                # --- INVESTMENTS: si existe, pinta y crea leyenda ---
                if has_inv:
                    fig_change.add_trace(go.Bar(
                        x=inv_tech[x_col],
                        y=inv_tech["Value"],
                        name=tech,
                        marker_color=color_map[tech],
                        legendgroup=tech,
                        showlegend=True      # leyenda por tecnología aquí
                    ))
                # --- DUMMY: solo si NO hay inv pero SÍ hay decommissions ---
                elif has_dec:
                    # x válido para la dummy: si x_col == "technology", usa la propia tecnología;
                    # en otro caso, usa el primer elemento de x_order (si existe).
                    dummy_x = [tech] if x_col == "technology" else ([x_order[0]] if x_order else [""])
                    fig_change.add_trace(go.Bar(
                        x=dummy_x,
                        y=[0],                         # barra invisible
                        name=tech,
                        marker_color=color_map[tech],  # color de la tecnología
                        legendgroup=tech,
                        showlegend=True,               # siembra la leyenda
                        marker_opacity=0.0,            # que no se vea
                        hoverinfo='skip'               # sin tooltip
                    ))

                # --- DECOMMISSIONS: pinta siempre que haya, pero sin crear leyenda ---
                if has_dec:
                    fig_change.add_trace(go.Bar(
                        x=dec_tech[x_col],
                        y=dec_tech["Value"],
                        name=tech,
                        marker_color=color_map[tech],
                        marker_pattern_shape="/",
                        legendgroup=tech,
                        showlegend=False               # no duplica leyenda
                    ))

            
            fig_change.update_layout(
                barmode='relative',
                height=height_value,
                bargap=0.35,
                template="plotly_white",
                legend_title_text="Technology",
                title=f"Invested (+) vs Decommissioned (-) by Technology — Year {year}",
                yaxis_title="Capacity (GW)"
            )
            fig_change.update_xaxes(categoryorder="array", categoryarray=x_order)
        
            st.plotly_chart(fig_change, width="stretch")

    else:
        fig_change = go.Figure()
        x_order = sorted(invested_data[x_col].dropna().unique())

        for tech in TECH_ORDER:
            inv_tech = invested_data[invested_data["technology"] == tech]
            dec_tech = decom_data[decom_data["technology"] == tech]
            
            if not inv_tech.empty:
                fig_change.add_trace(go.Bar(
                    x=inv_tech[x_col],
                    y=inv_tech["Value"],
                    name=tech,
                    marker_color=color_map[tech],
                    legendgroup=tech,
                    showlegend=True
                ))
            
            if not dec_tech.empty:
                fig_change.add_trace(go.Bar(
                    x=dec_tech[x_col],
                    y=dec_tech["Value"],
                    name=tech,
                    marker_color=color_map[tech],
                    marker_pattern_shape="/",
                    legendgroup=tech,
                    showlegend=False
                ))

        fig_change.update_layout(
            barmode='relative',
            height=fig_height,
            bargap=0.35,
            template="plotly_white",
            legend_title_text="Technology",
            title="Invested (+) vs Decommissioned (-) by Technology",
            yaxis_title="Capacity (GW)",
            xaxis_title=x_col.capitalize()
        )
        fig_change.update_xaxes(categoryorder="array", categoryarray=x_order)
        
        st.plotly_chart(fig_change, width="stretch")


# ----------------------------
# Plot: Installed Capacity Map (Optimized)
# ----------------------------

# Cache geodata loading
@st.cache_data
def load_geodata(path, poly_col):
    gdf = gpd.read_file(path).to_crs(epsg=4326)
    gdf['geometry'] = gdf['geometry'].simplify(tolerance=0.01)
    return json.loads(gdf.to_json()), gdf

POLY_COL = "id"
geojson_obj, gdf_base = load_geodata("onshore_PECD1.geojson", POLY_COL)

st.subheader("Installed Capacity Map Comparison")

# Filters on top
col1, col2 = st.columns(2)
with col1:
    map_year = st.selectbox("Year", years, index=0)
with col2:
    map_tech = st.selectbox("Technology", ["All Technologies"] + TECH_ORDER, index=0)

# Filter and aggregate data
df_cap = merged[(merged["scenario"] == scenario) & (merged["year"] == int(map_year))].copy()
if map_tech != "All Technologies":
    df_cap = df_cap[df_cap["technology"] == map_tech]

cap_by_polygon = df_cap.groupby("polygon", as_index=False)["Installed"].sum().rename(columns={"Installed": "Capacity (GW)"})

# Merge with geodata
gdf_plot = gdf_base[[POLY_COL, 'geometry']].merge(cap_by_polygon, left_on=POLY_COL, right_on="polygon", how="left")
gdf_plot["Capacity (GW)"] = gdf_plot["Capacity (GW)"].fillna(0)
max_cap = float(gdf_plot["Capacity (GW)"].max())

# Create map
fig_map = px.choropleth(
    gdf_plot, geojson=geojson_obj, locations=POLY_COL, featureidkey=f"properties.{POLY_COL}",
    color="Capacity (GW)", color_continuous_scale="Cividis", range_color=(0, max_cap if max_cap > 0 else 1),
    hover_name=POLY_COL, hover_data={"Capacity (GW)": ":.2f"}, projection="natural earth"
)

fig_map.update_geos(
    fitbounds="locations", visible=True, showframe=True, showcoastlines=True, showcountries=True,
    showocean=True, oceancolor="#1b2a34", showland=True, landcolor="#243647",
    lataxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.08)", dtick=5),
    lonaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.08)", dtick=5)
)

st.plotly_chart(fig_map, width="stretch")

if 'download_plot' in globals():
    download_plot(fig_map, "installed_capacity_map")


# ----------------------
# Plot 4: Storage
# ----------------------

def load_dill(path: str) -> dict:
    with open(path, "rb") as f:
        return dill.load(f)

# Cargar diccionario con escenarios
storage_dict = load_dill("files_out/node_state.dill")  # Ajusta la ruta

st.subheader("Node State by Storage Type and Country")

# --- Sidebar para filtros ---
scenario_storage = scenario
year_storage = year_option
storage_types = list(set([col.split("_")[0] for col in storage_dict[scenario_storage].columns]))
selected_storage_types = st.multiselect("Storage Types", storage_types, default=["reservoir"])

# --- Filtrar datos ---
df_storage = storage_dict[scenario_storage].copy()
if year_storage != "All Years":
    df_storage = df_storage[df_storage.index.year == int(year_storage if year_storage != "2040" else "2041")]

# Filtrar columnas por storage y país
cols_to_plot = []
for col in df_storage.columns:
    if len(col.split("_")) > 1 and "Europe" not in selected_countries:
        storage_type, country = col.split("_")
        if storage_type in selected_storage_types and country in selected_countries:
            cols_to_plot.append(col)
    else:
        storage_type = col.split("_")[0]
        if storage_type in selected_storage_types:
            cols_to_plot.append(col)

filtered_storage = df_storage[cols_to_plot]

# --- Plot con Plotly ---
if filtered_storage.empty:
    st.info("No data for selected filters.")
else:
    fig_storage = px.line(
        filtered_storage,
        x=filtered_storage.index,
        y=filtered_storage.columns,
        title=f"Node State for {', '.join(selected_storage_types)} in {', '.join(selected_countries)} ({scenario_storage})",
    )
    fig_storage.update_traces(connectgaps=True)
    fig_storage.update_layout(xaxis_title="Time",yaxis_title="Energy (MWh)",template="plotly_white", height=600, legend_title_text="Storage_Country")
    st.plotly_chart(fig_storage, width="stretch")

# -------------------------------
# Sankey Diagrams
# -------------------------------
st.subheader("Sankey Diagrams")

# Load flows
energy_flows = load_csv("files_out/energy_flows.csv")
crossborder_flows = load_csv("files_out/crossborder_flows.csv")
emissions_flows = load_csv("files_out/emissions_flows.csv")

col1, col2, col3 = st.columns(3)
with col1: 
    region_option = st.selectbox("Year for Sankey", ["Europe"] + countries)
with col2:
    year_sankey = st.selectbox("Year for Sankey", [str(y) for y in years])
with col3:
    sankey_type = st.selectbox("Sankey Type", ["Energy Flows", "Emissions Flows"])

def build_sankey(region, year, scenario_selected, flow_data, cb_data, title_prefix):
    year_col = f"y{year}"
    nodes = []
    links = []
    node_index = {}
    node_colors = []

    def add_node(name):
        if name not in node_index:
            node_index[name] = len(nodes)
            nodes.append(name)
            import hashlib
            hash_val = int(hashlib.md5(name.encode()).hexdigest(), 16)
            r = (hash_val % 180) + 75
            g = ((hash_val >> 8) % 180) + 75
            b = ((hash_val >> 16) % 180) + 75
            node_colors.append(f'rgb({r},{g},{b})')
        return node_index[name]

    if region == "Europe":
        df = flow_data[flow_data["scenario"] == scenario_selected].copy().groupby(["source","target"])[year_col].sum().reset_index()
        for _, row in df.iterrows():
            src, tgt, val = row["source"], row["target"], row[year_col]
            if val > 0.001:
                s_idx = add_node(src)
                t_idx = add_node(tgt)
                links.append({"source": s_idx, "target": t_idx, "value": val})
    else:
        df = flow_data[(flow_data["polygon"] == region)&(flow_data["scenario"] == scenario_selected)].copy().groupby(["source","target"])[year_col].sum().reset_index()
        for _, row in df.iterrows():
            src, tgt, val = row["source"], row["target"], row[year_col]
            if val > 0.001:
                s_idx = add_node(src)
                t_idx = add_node(tgt)
                links.append({"source": s_idx, "target": t_idx, "value": val})

        if cb_data is not None:
            cb_df = cb_data[(cb_data["source"] == region) | (cb_data["target"] == region)]
            for _, row in cb_df.iterrows():
                src_country, tgt_country, commodity, val = row["source"], row["target"], row["commodity"], row[year_col]
                if val > 0.001:
                    if tgt_country == region:
                        s_idx = add_node(f"Import-{src_country}")
                        t_idx = add_node(commodity)
                        links.append({"source": s_idx, "target": t_idx, "value": val})
                    elif src_country == region:
                        s_idx = add_node(commodity)
                        t_idx = add_node(f"Export-{tgt_country}")
                        links.append({"source": s_idx, "target": t_idx, "value": val})

    link_colors = [node_colors[link["source"]].replace('rgb', 'rgba').replace(')', ',0.4)') for link in links]

    fig = go.Figure(go.Sankey(
        node=dict(pad=15, thickness=20, line=dict(color="black", width=0.5),
                  label=nodes, color=node_colors),
        link=dict(source=[l["source"] for l in links],
                  target=[l["target"] for l in links],
                  value=[l["value"] for l in links],
                  color=link_colors)
    ))
    fig.update_layout(title_text=f"{title_prefix} - {region} - {year} - {scenario_selected}", 
                      font_size=12, height=1200)
    return fig

if year_sankey:
    if sankey_type == "Energy Flows":
        sankey_fig = build_sankey(region_option, int(year_sankey), scenario, 
                                   energy_flows, crossborder_flows, "Energy Flows Sankey")
    else:
        sankey_fig = build_sankey(region_option, int(year_sankey), scenario, 
                                   emissions_flows, None, "Emissions Flows Sankey")
    
    st.plotly_chart(sankey_fig, width="stretch")

    html_bytes = sankey_fig.to_html().encode()
    st.download_button(label="Download Sankey (HTML)", data=html_bytes,
                       file_name=f"sankey_{sankey_type.replace(' ', '_')}_{region_option}_{year_sankey}_{scenario}.html", 
                       mime="text/html")
    try:
        png_bytes = sankey_fig.to_image(format="png")
        st.download_button(label="Download Sankey (PNG)", data=png_bytes,
                           file_name=f"sankey_{sankey_type.replace(' ', '_')}_{region_option}_{year_sankey}_{scenario}.png", 
                           mime="image/png")
    except Exception:
        st.caption("PNG export requires `kaleido`. Install it via `pip install kaleido`.")

# ----------------------
# Download buttons
# ----------------------
st.markdown("### Download Plots")
def download_plot(fig, name):
    html_bytes = fig.to_html().encode()
    try:
        png_bytes = fig.to_image(format="png")
        st.download_button(label=f"Download {name} (PNG)", data=png_bytes,
                           file_name=f"{name}.png", mime="image/png")
    except Exception:
        st.caption("PNG export requires `kaleido`. Install it via `pip install kaleido`.")
    st.download_button(label=f"Download {name} (HTML)", data=html_bytes,
                       file_name=f"{name}.html", mime="text/html")

if 'fig_installed' in locals():
    download_plot(fig_installed, "installed_capacity")
if 'fig_flows' in locals():
    download_plot(fig_flows, "technology_flows")
if 'fig_change' in locals():
    download_plot(fig_change, "invested_vs_decommissioned")
if 'fig_map' in locals():
    download_plot(fig_map, "capacity_map")
if 'fig_storage' in locals():
    download_plot(fig_storage, "storage_state")
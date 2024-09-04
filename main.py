import inspect
import requests
import numpy as np
import pandas as pd
import streamlit as st
from st_snowauth import snowauth_session
from streamlit.components.v1 import html
from dash import Dash, dcc, html, Input, Output
from streamlit.errors import StreamlitAPIException
from streamlit_extras.echo_expander import echo_expander
from snowflake.snowpark.context import get_active_session
from streamlit_extras.metric_cards import style_metric_cards



st.markdown("## This (and above) is always seen")
def get_public_ip():
    ip = requests.get('https://aplicacion.streamlit.app/').text
    return ip

st.write("La dirección IP pública de esta aplicación es:", get_public_ip())
session = snowauth_session('OAUTHSNOW')


def change_mode():
    app_mode= "Información Detallada"

def open_page(url):
    open_script= """
        <script type="text/javascript">
            window.open('%s', '_blank').focus();
        </script>
    """ % (url)
    html(open_script)


metricas = """
SELECT 
SUM(TRX_REALES) AS TRX_REALES,
SUM(RECAUDADO) AS RECAUDADO,
COUNT(DISTINCT NOM_SERVICIO) AS NUM_SERVICIOS,
SUM(FACTURADO) AS FACTURADO
FROM SERVIPAG.RAW.TRANSACCIONES
"""
metricas_2 = """
SELECT SERVICIO,NUEVA_LINEA,ANOMES,
FECHA,CANAL,CLIENTE,
SUM(TRX_REALES) AS TRX_REALES,
SUM(RECAUDADO) AS RECAUDADO,
COUNT(DISTINCT NOM_SERVICIO) AS NUM_SERVICIOS,
SUM(FACTURADO) AS FACTURADO
FROM SERVIPAG.RAW.TRANSACCIONES
GROUP BY SERVICIO,NUEVA_LINEA,ANOMES,
FECHA,CANAL,CLIENTE
"""

@st.cache_data
def get_data(app_mode) -> pd.DataFrame:
    if app_mode == "Información Detallada":
        df_metricas = session.sql(metricas_2).collect()
    else:
        df_metricas = session.sql(metricas).collect()
    return pd.DataFrame(df_metricas)

def stylable_container(key, css_styles, wrapper_style=""):
    """
    Insert a container into your app which you can style using CSS.
    This is useful to style specific elements in your app.

    Args:
        key (str): The key associated with this container. This needs to be unique since all styles will be
            applied to the container with this key.
        css_styles (str | List[str]): The CSS styles to apply to the container elements.
            This can be a single CSS block or a list of CSS blocks.
        wrapper_style (str): (optional) Styles to apply to the wrapping container. Do not wrap in { }.

    Returns: A container object.
    """
    if isinstance(css_styles, str):
        css_styles = [css_styles]

    # Remove unneeded spacing that is added by the style markdown:
    css_styles.append(
        """
> div:first-child {
    display: none;
}
"""
    )

    style_text = """
<style>
"""
    if wrapper_style:
        style_text += f"""
    div[data-testid="stVerticalBlockBorderWrapper"]:has(
            > div
            > div[data-testid="stVerticalBlock"]
            > div.element-container
            > div.stMarkdown
            > div[data-testid="stMarkdownContainer"]
            > p
            > span.{key}
        ) {{
        {wrapper_style}
        }}
"""

    for style in css_styles:
        style_text += f"""

div[data-testid="stVerticalBlock"]:has(> div.element-container > div.stMarkdown > div[data-testid="stMarkdownContainer"] > p > span.{key}) {style}

"""

    style_text += f"""
    </style>

<span class="{key}"></span>
"""

    container = st.container()
    container.markdown(style_text, unsafe_allow_html=True)
    return container


class GridDeltaGenerator:
    def __init__(
        self,
        parent_dg,
        spec,
        *,
        gap = "small",
        repeat = True,
    ):
        self._parent_dg = parent_dg
        self._container_queue = []
        self._number_of_rows = 0
        self._spec = spec
        self._gap = gap
        self._repeat = repeat

    def _get_next_cell_container(self):
        if not self._container_queue:
            if not self._repeat and self._number_of_rows > 0:
                raise StreamlitAPIException("The row is already filled up.")

            # Create a new row using st.columns:
            self._number_of_rows += 1
            spec = self._spec[self._number_of_rows % len(self._spec) - 1]
            self._container_queue.extend(self._parent_dg.columns(spec, gap=self._gap))

        return self._container_queue.pop(0)

    def __getattr__(self, name):
        return getattr(self._get_next_cell_container(), name)


def grid(
    *spec,
    gap = "small",
    vertical_align = "top",
):
    """
    Insert a multi-element, grid container into your app.

    This function inserts a container into your app that arranges
    multiple elements in a grid layout as defined by the provided spec.
    Elements can be added to the returned container by calling methods directly
    on the returned object.

    Args:
        *spec (int | Iterable[int]): One or many row specs controlling the number and width of cells in each row.
            Each spec can be one of:
                * An integer specifying the number of cells. In this case, all cells have equal
                width.
                * An iterable of numbers (int or float) specifying the relative width of
                each cell. E.g., ``[0.7, 0.3]`` creates two cells, the first
                one occupying 70% of the available width and the second one 30%.
                Or, ``[1, 2, 3]`` creates three cells where the second one is twice
                as wide as the first one, and the third one is three times that width.
                The function iterates over the provided specs in a round-robin order. Upon filling a row,
                it moves on to the next spec, or the first spec if there are no
                more specs.
        gap (Optional[str], optional): The size of the gap between cells, specified as "small", "medium", or "large".
            This parameter defines the visual space between grid cells. Defaults to "small".
        vertical_align (Literal["top", "center", "bottom"], optional): The vertical alignment of the cells in the row.
            Defaults to "top".
    """

    container = stylable_container(
        key=f"grid_{vertical_align}",
        css_styles=[
            """
div[data-testid="column"] > div[data-testid="stVerticalBlockBorderWrapper"] > div {
height: 100%;
}
""",
            """
div[data-testid="column"] > div {
height: 100%;
}
""",
            f"""
div[data-testid="column"] > div[data-testid="stVerticalBlockBorderWrapper"] > div > div[data-testid="stVerticalBlock"] > div.element-container {{
    {"margin-top: auto;" if vertical_align in ["center", "bottom"] else ""}
    {"margin-bottom: auto;" if vertical_align == "center" else ""}
}}
""",
            f"""
div[data-testid="column"] > div > div[data-testid="stVerticalBlock"] > div.element-container {{
    {"margin-top: auto;" if vertical_align in ["center", "bottom"] else ""}
    {"margin-bottom: auto;" if vertical_align == "center" else ""}
}}
""",
        ],
    )

    return GridDeltaGenerator(
        parent_dg=container, spec=list(spec), gap=gap, repeat=True
    )


# Let's start by populating the sidebar.
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/d/d9/Logo_Servipag.svg")
    st.write("")
    app_mode = st.selectbox("Menú:", ["Resumen Empresa", "Información Detallada"])
    st.write("☝️ Menu de Selección!")

    # Now change its color using a css selector,
    # we can search for the sidebar, then change its css properties.
    st.markdown(
        """
        <style>
            [data-testid=stSidebar] {
                background-color: #7b858c;
                color: white;
            }
            [data-testid=stSidebarUserContent] {
                padding-top: 3.5rem;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )

def example_app():
    app_mode = "Resumen Empresa"
    df = get_data(app_mode)
    st.sidebar.markdown(
        """
        <style>
            div.block-container {
                padding: 0px;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )
    # Header section
    with stylable_container(
        key="title_container",
        css_styles="""
        {
            display: flex;
            padding-top: 40px;
            text-align: center;
            padding-bottom: 40px;
            flex-direction: row;
            align-items: center;
            box-sizing: border-box;
            background-color: #7b858c;
            justify-content: space-around;

            p {
                font-family: sans-serif;
            }

            > div:nth-child(2),
            > div:nth-child(3),
            > div:nth-child(4) {
                > div > div > div:nth-child(1) p {
                    color: #ffda02;
                    font-size: 44px;
                    font-weight: bold;
                }
                > div > div > div:nth-child(2) p {
                    color: #ffffff;
                    font-size: 18px;
                    font-weight: normal;
                }
            }
        }
            """,
        wrapper_style= """
            padding-left: 50px;
            padding-right: 50px;
            padding-bottom: 20px;
        """
    ):
        with st.container():
            st.write("{:,.3f} M".format(df.TRX_REALES.sum()/1000000))
            #st.write("9,437")
            st.write("Transacciones Totales")
        with st.container():
            st.write("{:,.3f} M".format(df.RECAUDADO.sum()/1000000000))
            #st.write("4.2B")
            st.write("Volumen Total Recaudado")
        with st.container():
            st.write("{:,.0f}".format(df.NUM_SERVICIOS.sum()))
            #st.write("2,416")
            st.write("Servicios Totales")

# 2ca94e
    with stylable_container(
        key="blue_title_text",
        css_styles="""
        {
            p {
                color: #ffda02; 
                font-size: 50px;
                font-weight: bold;
                text-align: center;
                font-family: sans-serif;
            }
        }
            """,
    ):
        st.write("Reporte Financiero")

    with stylable_container(
        key="black_title_text",
        css_styles="""
        {
            p {
                color: #ffffff;
                font-size: 50px;
                font-weight: bold;
                text-align: center;
                font-family: sans-serif;
            }
        }
            """,
    ):
        st.write("Con tecnología Servipag")

    with stylable_container(
        key="description_text",
        css_styles="""
        {
            p {
                color: #ffffff;
                font-size: 18px;
                text-align: center;
                font-family: sans-serif;
            }
        }
            """,
        wrapper_style="""
            padding: 0px 150px;
        """,
    ):
        st.write(
            """
            El informe detalla las transacciones realizadas y el volumen de recaudación 
            desglosado por canal de venta, proporcionando un análisis comparativo del 
            rendimiento financiero y la eficiencia de cada canal en el último período cerrado.
            """
        )

    # Icons section
    with stylable_container(
        key="icons_description_text",
        css_styles="""
        {
            p {
                color: #ffffff;
                font-size: 18px;
                font-style: italic;
                text-align: center;
                padding-top: 25px;
                padding-bottom: 25px;
                box-sizing: border-box;
                font-family: sans-serif;
            }
        }
            """,
    ):
        st.write("Más de 200 marcas han confiado en nuestros servicios")

    icons_arr = [
        "https://lowgrafic.cl/wp-content/uploads/2016/11/LOGO-ENTEL.png",
        "https://upload.wikimedia.org/wikipedia/commons/a/a9/The_DirecTV_logo.png",
        "https://upload.wikimedia.org/wikipedia/commons/thumb/3/33/WOM_Chile_logo.svg/2048px-WOM_Chile_logo.svg.png",
        "https://upload.wikimedia.org/wikipedia/commons/thumb/0/0c/Claro.svg/1200px-Claro.svg.png",
        "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTaS2Kuke1Y-kHYDIbggC6D3GJm8j4R4z2p8Q&s",
        "https://logodownload.org/wp-content/uploads/2018/12/movistar-logo.png",
        "https://www.globalvia.com/wp-content/uploads/2021/09/Logotipo_Autopista-Vespucio-Norte.png",
         "https://upsociative.com/wp-content/uploads/2023/11/cropped-menu-header-2.png",
        "https://pbs.twimg.com/profile_images/816348464816803840/kJNDjACH_400x400.jpg",
        "https://nuevo.sencillito.com/documents/61641/7706045/costanera-norte-autopista-pagar-en-linea-sencillito/bc53a6b9-a9b2-fa90-852e-7f459f9ee6fd",
    ]
    with stylable_container(
        key="row_1_icons_container",
        css_styles="""
            {
                display: flex;
                padding: 10px;
                flex-direction: row;
                align-items: center;
                justify-content: space-around;


                > div {
                    width: 140px;

                    > div > div {
                        width: 140px !important;
                    }
                }
            }
            """,
    ):
        st.image(icons_arr[0], width=140)
        st.image(icons_arr[1], width=140)
        st.image(icons_arr[2], width=140)
        st.image(icons_arr[3], width=140)
        st.image(icons_arr[4], width=140)

    with stylable_container(
        key="row_2_icons_container",
        css_styles="""
        {
            display: flex;
            padding: 10px;
            flex-direction: row;
            align-items: center;
            padding-bottom: 100px;
            box-sizing: border-box;
            justify-content: space-around;

            > div {
                width: 140px;

                > div > div {
                    width: 140px !important;
                }
            }
        }
            """,
    ):
        st.image(icons_arr[5], width=140)
        st.image(icons_arr[6], width=140)
        st.image(icons_arr[7], width=140)
        st.image(icons_arr[8], width=140)
        st.image(icons_arr[9], width=140)

    # Buttons section
    with stylable_container(
        key="buttons_container",
        css_styles="""
        {
            div[data-testid="stVerticalBlock"] {
                align-items: center;
            }
        }
            """,
    ):
        button1_col, button2_col = st.columns(2)

        with button1_col:
            with stylable_container(
                key="filled_button",
                css_styles="""
                {
                    button {
                        height: 56px;
                        width: 220px;
                        margin: 10px;
                        color: #ffffff;
                        border-radius: 25px;
                        text-transform: uppercase;
                        background-color: #ffda02;
                        border: 1px solid #ffda02;
                        }
                }
                    """,
            ):
                st.button("PORTAL SERVIPAG",on_click=open_page, args=('https://portal.servipag.com/',))

        with button2_col:
            with stylable_container(
                key="non_filled_button",
                css_styles="""
                {
                    button {
                        width: 220px;
                        margin: 10px;
                        height: 56px;
                        color: #ffda02;
                        border-radius: 50px;
                        text-transform: uppercase;
                        border: 1px solid #ffda02;
                        background-color: #ffffff;
                    }
                }
                    """,
            ):
                st.button("Información Detallada",on_click=change_mode)

    # Footer section
    with stylable_container(
        key="footer_container",
        css_styles="""
        {
            padding-top: 100px;
            box-sizing: border-box;

            .element-container {
                height: 70px;
                display: flex;
                color: #ffffff;
                text-align: center;
                align-items: center;
                background-color: #7b858c;

                a {
                    color: #ffffff;
                }
            }
        }
            """,
    ):
        st.write("Para más información, visite www.portal.servipag.com")


def custom_ui_features():
    # Clear the padding MD from other mode
    app_mode = "Información Detallada"
    st.sidebar.markdown("")

    st.title("✨ Información detallada de la empresa en Snowflake")

    """
    Bienvenido al panel de control de tu negocio. Aquí puedes encontrar un resumen visual y detallado de tu facturación y transacciones mensuales. 
    Explora los siguientes componentes para obtener una visión completa:
    - Facturación Mensual: Visualiza el total facturado cada mes en un gráfico de líneas o barras. Observa las tendencias y los picos de ingresos para evaluar el rendimiento de tus ventas.
    - Transacciones Mensuales:Consulta la cantidad de transacciones realizadas cada mes. El gráfico asociado te ayudará a identificar los períodos de mayor actividad y entender el comportamiento del cliente.
    - Tabla Pivotante de Transacciones y Recaudado:Examina la tabla pivotante para un análisis más detallado. Aquí podrás ver desglosado el número de transacciones y el monto recaudado por diferentes categorías, como tipo de producto o región. Ajusta los filtros para obtener la información que necesitas y obtener insights específicos.
    """
    st.caption("¡Navega por estas secciones para mantenerte al tanto del desempeño de tu negocio y tomar decisiones informadas!")

    #GET CURRENT SESSION ROLE
    sess_curr_role = session.get_current_role().strip('"')
    #QUERY SNOWFLAKE AND WRITE TO DATAFRAME
    app_context_sql = f"""
        SELECT
        CURRENT_ROLE() AS APP_QUERY_ROLE,
        '{sess_curr_role}' as APP_SESSION_ROLE
    """
    df_app_context = session.sql(app_context_sql).collect()
    #PRINT TO SCREEN
    st.write("Current App Context Roles:")
    st.dataframe(data=df_app_context)

    
    st.divider()
    df = get_data(app_mode)
    df_trx = pd.pivot_table(df, values='TRX_REALES', index=['FECHA'],
                       columns=['CANAL'], aggfunc="sum").reset_index()
    df_fact = pd.pivot_table(df, values='FACTURADO', index=['FECHA'],
                             columns=['CANAL'], aggfunc="sum").reset_index()
    
    random_df = pd.DataFrame(np.random.randn(20, 3), columns=["a", "b", "c"])

    my_grid = grid(1, 1, 1, 1, 1, 1, vertical_align="bottom")


    # Row 1:
    my_grid.selectbox("Empresa Seleccionada:", df.CLIENTE.unique().tolist())
                      #["Canales Digitales", "Servipag Express","Sucursales"])
    
    # Row 2:
    with my_grid.expander("Show Filters", expanded=False):
        min_canales, max_canales = st.select_slider("Canales digitales", 
                                            options=df_trx['Canales Digitales'].sort_values().dropna().unique(),
                                            value=(df_trx['Canales Digitales'].min(), df_trx['Canales Digitales'].max()))
        min_servipag, max_servipag = st.select_slider("Servipag Express", 
                                            options=df_trx['Servipag Express'].sort_values().dropna().unique(),
                                            value=(df_trx['Servipag Express'].min(), df_trx['Servipag Express'].max()))
        min_sucursales, max_sucursales = st.select_slider("Sucursales", 
                                            options=df_trx['Sucursales'].sort_values().dropna().unique(),
                                            value=(df_trx['Sucursales'].min(), df_trx['Sucursales'].max()))

        df_trx_filter = df_trx[(df_trx['Canales Digitales'] >= min_canales) & (df_trx['Canales Digitales'] <= max_canales) 
                        & (df_trx['Servipag Express'] >= min_servipag) & (df_trx['Servipag Express'] <= max_servipag) 
                        & (df_trx['Sucursales'] >= min_sucursales) & (df_trx['Sucursales'] <= max_sucursales)]

    # Row 3:
    my_grid.line_chart(
    df_trx_filter,
    x="FECHA",
    y=["Canales Digitales", "Servipag Express","Sucursales"],
    color=["#FF0000", "#0000FF","#ffde00"], # Optional
    use_container_width=True
    )
    # Row 4:
    with my_grid.expander("Show Filters", expanded=False):
        min_canales_fact, max_canales_fact = st.select_slider("Canales digitales", 
                                            options=df_fact['Canales Digitales'].sort_values().dropna().unique(),
                                            value=(df_fact['Canales Digitales'].min(), df_fact['Canales Digitales'].max()))
        min_servipag_fact, max_servipag_fact = st.select_slider("Servipag Express", 
                                            options=df_fact['Servipag Express'].sort_values().dropna().unique(),
                                            value=(df_fact['Servipag Express'].min(), df_fact['Servipag Express'].max()))
        min_sucursales_fact, max_sucursales_fact = st.select_slider("Sucursales", 
                                            options=df_fact['Sucursales'].sort_values().dropna().unique(),
                                            value=(df_fact['Sucursales'].min(), df_fact['Sucursales'].max()))
    
        
        df_fact_filter = df_fact[(df_fact['Canales Digitales'] >= min_canales_fact) & (df_fact['Canales Digitales'] <= max_canales_fact)
                            & (df_fact['Servipag Express'] >= min_servipag_fact) & (df_fact['Servipag Express'] <= max_servipag_fact)
                            & (df_fact['Sucursales'] >= min_sucursales_fact) & (df_fact['Sucursales'] <= max_sucursales_fact)]
    # Row 5:
    my_grid.line_chart(
    df_fact_filter,
    x="FECHA",
    y=["Canales Digitales", "Servipag Express","Sucursales"],
    color=["#FF0000", "#0000FF","#ffde00"], # Optional
    use_container_width=True
    )
    # Row 6:
    df['DIA'] = df.FECHA.str[-2:].astype(int)
    df1 = pd.pivot_table(df, values=['TRX_REALES', 'RECAUDADO'], index=['ANOMES','DIA','NUEVA_LINEA'],
    aggfunc={'TRX_REALES': "sum", 'RECAUDADO': "mean"}).reset_index().pivot(index=['ANOMES','NUEVA_LINEA'], 
    columns='DIA', values=['TRX_REALES', 'RECAUDADO'])
    my_grid.dataframe(df1, use_container_width=True)

    
if app_mode == "Resumen Empresa":
    example_app()
elif app_mode == "Información Detallada":
    custom_ui_features()
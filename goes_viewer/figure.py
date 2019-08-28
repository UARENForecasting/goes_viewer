import os


from bokeh.plotting import figure
from bokeh.models import (
    WMTSTileSource,
    Slider,
    ColumnDataSource,
    CustomJS,
    AjaxDataSource,
    RadioButtonGroup,
)
from bokeh.resources import CDN
from bokeh.embed import file_html
from bokeh.io import curdoc
from jinja2 import Environment, PackageLoader
from pyproj import transform


from goes_viewer import config
from goes_viewer.constants import WEB_MERCATOR, G17_CORNERS, DX, DY


def compute_image_locations_ranges(corners, range_lon_limits, range_lat_limits):
    xn, yn = transform(
        WEB_MERCATOR.geodetic_crs,
        WEB_MERCATOR,
        sorted(corners[:, 0]),
        sorted(corners[:, 1]),
        always_xy=True,
    )
    x_range, y_range = transform(
        WEB_MERCATOR.geodetic_crs,
        WEB_MERCATOR,
        range_lon_limits,
        range_lat_limits,
        always_xy=True,
    )
    y = yn[-1] - DY / 2
    x = xn[0] - DX / 2
    w = xn[-1] - xn[0]
    h = yn[-1] - yn[0]
    scale = (x_range[1] - x_range[0]) / (y_range[1] - y_range[0])
    return {"x": x, "y": y, "w": w, "h": h}, x_range, y_range, scale


# Need to use this and not bokeh.tile_providers.STAMEN_TONER
# https://github.com/bokeh/bokeh/issues/4770
STAMEN_TONER = WMTSTileSource(
    url=(config.TILE_SOURCE + "/{Z}/{X}/{Y}.png"),
    attribution=(
        'Map tiles by <a href="http://stamen.com">Stamen Design</a>, '
        'under <a href="http://creativecommons.org/licenses/by/3.0">CC BY 3.0'
        '</a>. Map data by <a href="http://openstreetmap.org">OpenStreetMap'
        '</a>, under <a href="http://www.openstreetmap.org/copyright">ODbL</a>'
    ),
)


def create_bokeh_figure(
    corners,
    lon_limits,
    lat_limits,
    base_url="figs/",
    base_height=800,
    image_alpha=0.8,
    title="GOES GeoColor Imagery at ",
):
    img_args, x_range, y_range, scale = compute_image_locations_ranges(
        corners, lon_limits, lat_limits
    )
    map_fig = figure(
        plot_width=int(scale * base_height),
        plot_height=base_height,
        x_axis_type="mercator",
        y_axis_type="mercator",
        x_range=x_range,
        y_range=y_range,
        title=title,
        toolbar_location="right",
        sizing_mode="scale_width",
        name="map_fig",
        tooltips=[("Site", "@name")],
    )

    slider = Slider(
        title="GOES Image",
        start=0,
        end=100,
        value=0,
        name="timeslider",
        sizing_mode="scale_width",
    )

    play_buttons = RadioButtonGroup(
        labels=["\u25B6", "\u25FC", "\u27F3"],
        active=1,
        name="play_buttons",
        sizing_mode="scale_width",
    )
    fig_source = ColumnDataSource(data=dict(url=[]), name="figsource", id="figsource")
    adapter = CustomJS(
        args=dict(
            slider=slider, fig_source=fig_source, base_url=base_url, title=map_fig.title
        ),
        code="""
    const result = {url: []}
    const urls = cb_data.response
    var pnglen = 0;
    for (i=0; i<urls.length; i++) {
        var name = urls[i]['name'];
        if (name.endsWith('.png')) {
            result.url.push(base_url + name)
            pnglen += 1
        }
    }
    slider.end = Math.max(pnglen - 1, 1)
    slider.change.emit()
    if (fig_source.data['url'].length == 0) {
        fig_source.data['url'][0] = result.url[0]
        fig_source.tags = ['']
        fig_source.change.emit()
    }
    return result
    """,
    )
    url_source = AjaxDataSource(
        data_url=base_url, polling_interval=10000, adapter=adapter
    )
    url_source.method = "GET"
    # url_source.if_modified = True

    pt_adapter = CustomJS(
        code="""
    const result = {x: [], y: [], name: []}
    const pts = cb_data.response
    for (i=0; i<pts.length; i++) {
        result.x.push(pts[i]['x'])
        result.y.push(pts[i]['y'])
        result.name.push(pts[i]['name'])
    }
    return result
"""
    )

    pt_source = AjaxDataSource(
        data_url=base_url + "/metadata.json",
        polling_interval=int(1e5),
        adapter=pt_adapter,
    )
    pt_source.method = "GET"

    callback = CustomJS(
        args=dict(fig_source=fig_source, url_source=url_source),
        code="""
        if (cb_obj.value < url_source.data['url'].length){
            var inp_url = url_source.data['url'][cb_obj.value];
            fig_source.data['url'][0] = inp_url;
            fig_source.tags = [cb_obj.value];
            fig_source.change.emit();
        }
        """,
    )

    title_callback = CustomJS(
        args=dict(title=map_fig.title, base_title=title),
        code="""
        var url = cb_obj.data['url'][0]
        var date = url.split('/').pop().split('_').pop().split('.')[0]
        title.text = base_title + date
        title.change.emit()
        """,
    )

    play_callback = CustomJS(
        args=dict(slider=slider, play_speed=config.PLAY_SPEED),
        code="""
    function stop() {
        var id = cb_obj._id
        clearInterval(id)
        cb_obj.active = 1
    }

    function advance() {
        if (slider.value < slider.end) {
            slider.value += 1
        } else {
            slider.value = 0
        }
        slider.change.emit()
    }

    function start() {
        var id = setInterval(advance, play_speed)
        cb_obj._id = id
    }

    if (cb_obj.active == 0) {
        start()
    } else if (cb_obj.active == 2) {
        stop()
        slider.value = 0
        slider.change.emit()
    } else {
        stop()
    }
    """,
    )

    # ajaxdatasource to nginx list of files as json possibly on s3
    # write metadata for plants to file, serve w/ nginx
    # new goes image triggers lambda to process sns -> sqs -> lambda
    map_fig.add_tile(STAMEN_TONER)
    map_fig.image_url(
        url="url", global_alpha=image_alpha, source=fig_source, **img_args
    )
    fig_source.js_on_change("tags", title_callback)
    map_fig.cross(
        x="x", y="y", size=12, fill_alpha=0.9, source=pt_source, color=config.RED
    )
    slider.js_on_change("value", callback)
    play_buttons.js_on_change("active", play_callback)
    doc = curdoc()
    for thing in (map_fig, play_buttons, slider):
        doc.add_root(thing)
    doc.title = "GOES Image Viewer"
    return doc


def render_html():
    doc = create_bokeh_figure(G17_CORNERS, config.LON_LIMITS, config.LAT_LIMITS)
    env = Environment(loader=PackageLoader("goes_viewer", "templates"))
    template = env.get_template("index.html")
    html = file_html(doc, CDN, "GOES Image Viewer", template)
    with open(config.FILENAME, "w") as f:
        f.write(html)


if __name__ == "__main__":
    render_html()

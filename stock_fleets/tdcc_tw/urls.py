from tdcc_tw import views

# method ,path ,tag ,view
routeList = (
    ('get', '/tdcc_search/', "tdcc", views.read_tdcc),
    ('get', '/tdcc_strategy/', "tdcc", views.tdcc_strategy),
)

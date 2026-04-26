from dash import Dash, html

app = Dash(__name__)
app.title = "Misinfo Analytics Dashboard"

app.layout = html.Div(
	style={
		"fontFamily": "Arial, sans-serif",
		"padding": "2rem",
		"maxWidth": "900px",
		"margin": "0 auto",
	},
	children=[
		html.H1("Misinformation Analytics Dashboard"),
		html.P("Dashboard service is up. Visual analytics panels will be added in Phase 5."),
	],
)


if __name__ == "__main__":
	app.run(host="0.0.0.0", port=8050)

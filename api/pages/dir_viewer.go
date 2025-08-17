package pages

var DirViewerPage = `
		<!DOCTYPE html>
		<html>
		<head>
			<title>Directory Listing: %s</title>
			<style>
				body { font-family: -apple-system, BlinkMacSystemFont, \"Segoe UI\", Roboto, Helvetica, Arial, sans-serif; margin: 20px; color: #333; }
				h2 { border-bottom: 1px solid #ccc; padding-bottom: 10px; }
				a { color: #007bff; text-decoration: none; }
				a:hover { text-decoration: underline; }
				table { border-collapse: collapse; width: 100%%; margin-top: 20px; }
				th, td { text-align: left; padding: 8px; border-bottom: 1px solid #ddd; }
				th { background-color: #f2f2f2; }
				.download-all { display: inline-block; margin-bottom: 20px; padding: 10px 20px; background-color: #28a745; color: white; border-radius: 5px; font-weight: bold; }
				.download-all:hover { background-color: #218838; }
			</style>
		</head>
		<body>
			<h2>Directory: %s</h2>
			<a href="/download/%s" class="download-all">Download All as .zip</a>
			<table>
				<tr>
					<th>Type</th>
					<th>Name</th>
					<th>Size</th>
					<th>Hash</th>
				</tr>
	`

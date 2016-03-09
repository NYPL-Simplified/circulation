admin = """
<!doctype html>
<html>
<head><title>Circulation Manager</title></head>
<body>
  <script src=\"/admin/static/circulation-web.js\"></script>
  <script>
    var circulationWeb = new CirculationWeb({
        csrfToken: \"{{ csrf_token }}\",
        homeUrl: \"{{ home_url }}\"
    });
  </script>
</body>
</html>
"""

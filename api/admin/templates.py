admin = """
<!doctype html>
<html>
<head><title>Circulation Manager</title></head>
<body>
  <script src=\"/admin/static/circulation-web.js\"></script>
  <script>
    var circulationWeb = new CirculationWeb(
      {% if csrf_token %}
        \"{{ csrf_token }}\"
      {% endif %}
    );
  </script>
</body>
</html>
"""

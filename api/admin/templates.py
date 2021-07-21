admin = """
<!doctype html>
<html>
  {% if \"/admin/static/circulation-web.css\" == null or \"/admin/static/circulation-web.js\" == null %}
  <head>
    <title>Circulation Manager</title>
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  </head>
  <body>
    <p>You probably forgot to run npm install</p>
  </body>
</html>
  {% else %}
  <head>
    <title>Circulation Manager</title>
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
    <link href=\"/admin/static/circulation-web.css\" rel="stylesheet" />
  </head>
  <body>
    <script src=\"/admin/static/circulation-web.js\"></script>
    <script>
      var circulationWeb = new CirculationWeb({
        csrfToken: \"{{ csrf_token }}\",
        tos_link_href: \"{{ sitewide_tos_href }}\",
        tos_link_text: \"{{ sitewide_tos_text }}\",
        showCircEventsDownload: {{ "true" if show_circ_events_download else "false" }},
        settingUp: {{ "true" if setting_up else "false" }},
        email: \"{{ email }}\",
        roles: [{% for role in roles %}{"role": \"{{role.role}}\"{% if role.library %}, "library": \"{{role.library.short_name}}\"{% endif %} },{% endfor %}]
    });
    </script>
  </body>
  {% endif %}
</html>
"""

admin_sign_in_again = """
<!doctype html>
<html>
<head><title>Circulation Manager</title></head>
<body>
  <p>You are now logged in. You may close this window and try your request again.
</body>
</html>
"""

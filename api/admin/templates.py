admin = """
<!doctype html>
<html>
<head>
<title>Circulation Manager</title>
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
<link href=\"/admin/static/circulation-web.css\" rel="stylesheet"/>
<style>
  .error {
    color: #1B7FA7;
    font-family: sans-serif;
    margin-left: 30px
  }
</style>
</head>
<body>
  <p class="error" id="error1" style="font-weight:bold;font-size:x-large;margin-top:50px"></p>
  <p class="error" id="error2" style="font-size:medium;margin-top:10px"></p>
  <script src=\"/admin/static/circulation-web.js\"></script>
  <script>
  try {
    var circulationWeb = new CirculationWeb({
        csrfToken: \"{{ csrf_token }}\",
        tos_link_href: \"{{ sitewide_tos_href }}\",
        tos_link_text: \"{{ sitewide_tos_text }}\",
        showCircEventsDownload: {{ "true" if show_circ_events_download else "false" }},
        settingUp: {{ "true" if setting_up else "false" }},
        email: \"{{ email }}\",
        roles: [{% for role in roles %}{"role": \"{{role.role}}\"{% if role.library %}, "library": \"{{role.library.short_name}}\"{% endif %} },{% endfor %}]
    });
    const elementsToRemove = document.getElementsByClassName("error");
    while(elementsToRemove.length > 0){
        elementsToRemove[0].parentNode.removeChild(elementsToRemove[0]);
    }
  } catch (e) {
    document.getElementById("error1").innerHTML = "We're having trouble displaying this page."
    document.getElementById("error2").innerHTML = "Contact your administrator, and ask them to check the console for more information."
    console.error("The following error occurred: ", e)
    console.warn("The CSS and/or JavaScript files for this page could not be found. Try running `npm install` in the api/admin directory of the circulation repo.")
  }
  </script>
</body>
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

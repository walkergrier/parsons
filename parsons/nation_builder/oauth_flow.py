# import requests
# from urllib.parse import urlencode
# from flask import Flask, request, redirect
# from pyngrok import ngrok
# # import webbrowser

# # NationBuilder Configuration (Replace with your actual values)
# NATION_SLUG = "your_nation_slug"
# CLIENT_ID = "your_client_id"
# CLIENT_SECRET = "your_client_secret"
# REDIRECT_URI = "http://localhost:5000/callback"  # Initial local URI

# app = Flask(__name__)
# authorization_code = None
# access_token = None
# refresh_token = None


# @app.route("/callback")
# def callback():
#     global authorization_code
#     authorization_code = request.args.get("code")
#     if authorization_code:
#         return "Authorization code received. Check your terminal."
#     else:
#         error = request.args.get("error")
# #         return f"Error: {error}"


# def exchange_code_for_token(code):
#     token_url = f"https://{NATION_SLUG}.nationbuilder.com/oauth/token"
#     payload = {
#         "grant_type": "authorization_code",
#         "code": code,
#         "client_id": CLIENT_ID,
#         "client_secret": CLIENT_SECRET,
#         "redirect_uri": REDIRECT_URI,
#     }
#     headers = {"Content-Type": "application/json", "Accept": "application/json"}
#     response = requests.post(token_url, json=payload, headers=headers)

#     if response.status_code == 200:
#         token_data = response.json()
#         global access_token, refresh_token
#         access_token = token_data.get("access_token")
#         refresh_token = token_data.get("refresh_token")
#         print("Access Token:", access_token)
#         print("Refresh Token:", refresh_token)
#     else:
#         print("Token exchange failed:", response.text)


# def start_oauth_flow():
#     global REDIRECT_URI
#     # Start ngrok tunnel
#     tunnel = ngrok.connect(5000)
#     REDIRECT_URI = tunnel.public_url + "/callback"
#     print(
#         "Enter URI as callback in NationBuilder App Registration",None,
#         REDIRECT_URI,
#         sep="\n",
#     )

#     k = 0
#     while t != 1:
#         print(
#             "Enter URI as 'callback url' in NationBuilder App Registration",
#             REDIRECT_URI,
#             None,
#             "Please choose from the following options:",
#             "> 1 | URI has been registered, continue with API Key setup",
#             "> 2 | End API Key setup"
#         )

#         try:
#             t = input("Select choice: ")
#         except:
#             pass

#         if t == 2:
#             ngrok.kill()
#             print("API Key setup ended.")

#     # Construct Authorization URL
#     auth_params = urlencode({
#         "response_type": "code",
#         "client_id": CLIENT_ID,
#         "redirect_uri": REDIRECT_URI
#     })
#     auth_url = f"https://{NATION_SLUG}.nationbuilder.com/oauth/authorize?" + auth_params
#     print(
        
#         f"Authorization URL: {auth_url}"
#     )

#     # NOTE: I'm not sure if I want to do this.
#     # Open the authorization URL in the default web browser
#     # webbrowser.open(authorization_url)

#     # Run the Flask app
#     app.run(port=5000)

#     # after the app.run exits, meaning the callback was recieved, stop ngrok.
#     ngrok.kill()
#     if authorization_code:
#         exchange_code_for_token(authorization_code)


# if __name__ == "__main__":
#     start_oauth_flow()

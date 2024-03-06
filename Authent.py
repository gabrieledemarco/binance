import streamlit as st
import streamlit_authenticator as stauth
import traceback
import sys

names = ['John Smith', 'Rebecca Briggs']
usernames = ['jsmith', 'rbriggs']
passwords = ['123', '456']


def main():
    try:
        hashed_passwords = stauth.hasher(passwords).generate()
        authenticator = stauth.authenticate(names, usernames, hashed_passwords,
                                            'some_cookie_name', 'some_signature_key', cookie_expiry_days=30)
        name, authentication_status = authenticator.login('Login', 'main')

        if authentication_status is False:
            st.error("Username or Password are incorrect")
        elif authentication_status is True:
            st.success(f"User Authorized, welcome dear {name} ")
        elif authentication_status is None:
            st.info("Please access to your account with a Username and Password")

    except Exception:
        print("")


main()

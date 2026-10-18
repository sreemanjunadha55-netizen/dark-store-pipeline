# Databricks notebook source
token = dbutils.secrets.get(scope="kv-darkstore", key="demo-alert-token")
print("Secret length:", len(token))
print(token)
-- Databricks notebook source
CREATE WIDGET TEXT name DEFAULT 'Amit'

-- COMMAND ----------

CREATE WIDGET DROPDOWN gender DEFAULT 'male' CHOICES SELECT 'male'

-- COMMAND ----------

REMOVE WIDGET name

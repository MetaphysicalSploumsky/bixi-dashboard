# Montreal BIXI Real-Time Monitoring Platform

### [View Live Dashboard](#) *(Insert link if active)*

A full-stack data engineering project designed to track, archive, and visualize the status of Montreal's BIXI bike-share network in real-time. Built to assist commuters during the 2025 Montreal Transit Strike by identifying system availability trends and station-specific stress metrics.

## 🏗 Architecture

The system moves from a local script-based approach to a fully cloud-native architecture hosted on AWS.

```mermaid
graph LR
    A[BIXI API (GBFS)] -->|JSON| B[AWS Lambda]
    B -->|Docker Image| C[AWS ECR]
    B -->|Write Processed Data| D[(AWS RDS PostgreSQL)]
    D -->|SQL Queries| E[Streamlit Dashboard]
    E -->|Visuals| F[End User]

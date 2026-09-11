## 📊 Tableau Analytics & Visualization

The processed Gold layer data was connected to Tableau to uncover spatial and financial insights from the NYC Taxi trip dataset. Below are the key findings derived from our 10-batch sample analysis:

### 1. Spatial Pickup Density (Geo Map)
* **Business Question:** Where and when do the highest concentrations of taxi pickups occur across New York City?
* **Key Insight:** Pickup density is heavily concentrated in Manhattan’s commercial areas, while outer boroughs and airports appear as scattered, lower-density points.
<img width="1430" height="732" alt="geo map" src="https://github.com/user-attachments/assets/74506e70-7009-401b-9716-f0a5819ccf3a" />

### 2. Peak Operational Hours
* **Business Question:** What are the peak operational hours during the day?
* **Key Insight:** Trip volume peaks significantly during evening rush hours, showing a sharp surge around **7 PM (hour 19)**.
* <img width="1418" height="716" alt="busiest hour" src="https://github.com/user-attachments/assets/245b592d-ed06-40a6-9b35-3aa10d8b9c11" />


### 3. Fare vs. Total Revenue Dynamics
* **Business Question:** Are high-revenue regions driven by expensive individual trips or by high trip volume?
* **Key Insight:** Some spatial cells generate exceptionally high total fares despite a lower average fare per trip, highlighting **high-volume zones** rather than just expensive individual trips. This distinction became visible due to our Gold layer aggregation design (tracking totals and counts rather than pre-averaged metrics).
* <img width="1441" height="721" alt="fare sv trip" src="https://github.com/user-attachments/assets/8ba2bee2-df83-4a8d-91f6-40861432492c" />

### Final Dashboard

<img width="1497" height="676" alt="tableau dashboard" src="https://github.com/user-attachments/assets/062b9ac1-73ad-4b50-a80c-489dd3024233" />

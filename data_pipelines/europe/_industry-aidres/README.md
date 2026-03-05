# AIDRES industrial dataset

AIDRES original data is in a post-qre server hosted by VITO. The data has been dumped to an excel file using an R script. At present, the data can be read from the Excel file using the aidres_from_excel.json Spine Toolbox data specification. The data should be deposited in a database that has the data structure from industry_aidres.sqlite (copy to your workflow).

The data is at NUTS3 level for the consumption of industrial products. There are also multiple pathways to produce these products along with their energy requirements, capital costs and co2 emissions. The technology pathway parameters are European-wide.
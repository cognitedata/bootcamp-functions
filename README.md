## bootcamp-functions

This repository contains the code that is run using Cognite Functions for the [CDF Enablement Bootcamp](https://docs-bootcamp.app.cogniteapp.com/)
(hereafter referred to as _Bootcamp_).

**Note:** This repository was generated from the template repository [cognitedata/deploy-functions-oidc](https://github.com/cognitedata/deploy-functions-oidc).
Information about how to use and deploy code/models to Cognite Functions within a CI/CD pipeline using OIDC can be found
in the template repo's README.
For more information about Cognite Functions, refer to the [Cognite documentation](https://docs.cognite.com/cdf/functions/).

# execute_rest_extractor

This function runs the [ice-cream-factory-datapoints-extractor](https://github.com/cognitedata/python-extractor-example/tree/fix_backfil/ice-cream-factory-datapoints-extractor)
to create timeseries (if it doesn't already exist in the CDF project) and extracts datapoints data from the
[ice-cream-factory-api](https://ice-cream-factory.inso-internal.cognite.ai/docs#/) into CDF clean.

While a Cognite Function is the not recommended for running extractors, it is a suitable tool to demonstrate an
extraction pipeline in the Bootcamp.

# oee_timeseries

This function calculates the overall equipment effectiveness (OEE) using the values from timeseries extracted above,
primarily:

* good
* count
* status
* planned status

More details on these timeseries and the calculation is found in the [CDF Enablement Bootcamp](https://docs-bootcamp.app.cogniteapp.com/) documentation.

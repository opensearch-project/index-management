[![Test and Build Workflow](https://github.com/opensearch-project/index-management/workflows/Test%20and%20Build%20Workflow/badge.svg)](https://github.com/opensearch-project/index-management/actions)
[![codecov](https://codecov.io/gh/opensearch-project/index-management/branch/main/graph/badge.svg)](https://codecov.io/gh/opensearch-project/index-management)
[![Roadmap](https://img.shields.io/badge/roadmap-checkout-ff69b4)](https://github.com/opensearch-project/index-management/projects/1)
[![Documentation](https://img.shields.io/badge/api-reference-blue.svg)](https://opensearch.org/docs/im-plugin/index/)
[![Chat](https://img.shields.io/badge/chat-on%20forums-blue)](https://discuss.opendistrocommunity.dev/c/index-management/)
![PRs welcome!](https://img.shields.io/badge/PRs-welcome!-success)

<img src="https://opensearch.org/assets/brand/SVG/Logo/opensearch_logo_default.svg" height="64px"/>

- [OpenSearch Index Management](#opensearch-index-management)
  - [Highlights](#highlights)
- [Contributing](#contributing)
- [Getting Help](#getting-help)
- [Code of Conduct](#code-of-conduct)
- [Security](#security)
- [License](#license)
- [Copyright](#copyright)

# OpenSearch Index Management

OpenSearch Index Management provides a suite of features to monitor and manage indexes.

It currently contains an automated system for managing and optimizing indices throughout their life, Index State Management.

View the original [request for comments](docs/rfc.md).

## Highlights

With Index State Management you will be able to define custom policies, to optimize and manage indices and apply them to index patterns.

Each policy contains a default state and a list of states that you define for the index to transition between.

Within each state you can define a list of actions to perform and transitions to enter a new state based off certain conditions.

The current supported actions are:

* Delete
* Close
* Open
* Force merge
* Notification
* Read only
* Read write
* Replica count
* Rollover

The current supported transition conditions are:

* Index doc count
* Index size
* Index age
* Cron expression

## Contributing

See [developer guide](DEVELOPER_GUIDE.md) and [how to contribute to this project](CONTRIBUTING.md).

## Getting Help

If you find a bug, or have a feature request, please don't hesitate to open an issue in this repository.

For more information, see [project website](https://opensearch.org/) and [documentation](https://opensearch.org/docs/). If you need help and are unsure where to open an issue, try [forums](https://discuss.opendistrocommunity.dev/).

## Code of Conduct

This project has adopted the [Amazon Open Source Code of Conduct](CODE_OF_CONDUCT.md). For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq), or contact [opensource-codeofconduct@amazon.com](mailto:opensource-codeofconduct@amazon.com) with any additional questions or comments.

## Security

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.

## License

This project is licensed under the [Apache v2.0 License](./LICENSE)

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](NOTICE) for details.

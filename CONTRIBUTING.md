# Contributing Guidelines

Thank you for your interest in contributing to our project. Whether it's a bug report,
new feature, correction, or additional documentation, we greatly value feedback and
contributions from our community.

Please read through this document before submitting any issues or pull requests to ensure
we have all the necessary information to effectively respond to your bug report or
contribution.

## Reporting Bugs/Feature Requests

We welcome you to use the GitHub issue tracker to report bugs or suggest features.

When filing an issue, please check existing open, or recently closed, issues to make sure
somebody else hasn't already reported the issue. Please try to include as much information
as you can. Details like these are incredibly useful:

- A reproducible test case or series of steps
- The version of our code being used
- Any modifications you've made relevant to the bug
- Anything unusual about your environment or deployment

**Do not include real account IDs, ARNs, Amazon Cognito user pool IDs, API endpoints, or
real Vehicle Identification Numbers (VINs) in issues or pull requests.** This project
processes personal data by design; please redact identifiers before sharing logs. The
scripts in `deploy/` already mask VINs before writing them to logs — see `mask_vin()` in
`deploy/assets/lambda/data_api/index.py`.

## Contributing via Pull Requests

Contributions via pull requests are much appreciated. Before sending us a pull request,
please ensure that:

1. You are working against the latest source on the *main* branch.
2. You check existing open, and recently merged, pull requests to make sure someone else
   hasn't addressed the problem already.
3. You open an issue to discuss any significant work — we would hate for your time to be
   wasted.

To send us a pull request, please:

1. Fork the repository.
2. Modify the source; please focus on the specific change you are contributing. If you
   also reformat all the code, it will be hard for us to focus on your change.
3. Ensure the deployment gates pass locally. `deploy/deploy.sh` runs them in order, but you
   can run them individually:
   ```bash
   cd deploy
   npx tsc --noEmit                              # TypeScript type check
   python3 -m unittest discover -s tests -t . -q # API authorization and input validation
   npx cdk synth --quiet                         # asset build, incl. Flink unit tests
   ```
4. Commit to your fork using clear commit messages.
5. Send us a pull request, answering any default questions in the pull request interface.
6. Pay attention to any automated CI failures reported in the pull request, and stay
   involved in the conversation.

GitHub provides additional documentation on
[forking a repository](https://help.github.com/articles/fork-a-repo/) and
[creating a pull request](https://help.github.com/articles/creating-a-pull-request/).

## Notes specific to this project

- **Compliance-relevant behavior is covered by tests, not comments.** The tests in
  `deploy/tests/test_data_api.py` assert security properties: no privilege escalation, no
  cross-tenant access to asynchronous jobs, no plaintext VINs in logs. If your change
  touches authorization or erasure, add an assertion rather than a comment.
- **Sample data must stay synthetic.** VINs are generated with the deliberately
  non-manufacturer prefix `SAMPLEVEH0` (see `VIN_PREFIX` in
  `deploy/assets/lambda/kafka_tools/producer.py`). Do not substitute a real World
  Manufacturer Identifier.
- **Table properties are load-bearing.** `write.delete.mode=copy-on-write` is what makes
  erasure physical rather than logical. Changing table properties requires re-running the
  erasure validation in `deploy/validate-api.sh`.

## Finding contributions to work on

Looking at the existing issues is a great way to find something to contribute on. Issues
labeled 'help wanted' are a great place to start.

## Code of Conduct

This project has adopted the
[Amazon Open Source Code of Conduct](https://aws.github.io/code-of-conduct).
See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## Security issue notifications

If you discover a potential security issue in this project we ask that you notify AWS/Amazon
Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/).
Please do **not** create a public GitHub issue.

## Licensing

See the [LICENSE](LICENSE) file for our project's licensing. We will ask you to confirm the
licensing of your contribution.

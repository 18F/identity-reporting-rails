## Security

### Security architecture

We are utilizing the industry's best security practices with guidance from NIST and the latest [Digital Authentication Guidelines](https://pages.nist.gov/800-63-3/sp800-63-3.html).

Our application is continuously monitored for CVE, OSVDB, XSS, SQL injection and many other types of vulnerabilities using [Snyk](https://snyk.io).


### Operations

The application and server-level health and availability is monitored using [New Relic](https://newrelic.com) and incident response is handled using [Opsgenie](https://www.atlassian.com/software/opsgenie).

We implemented our own independent monitoring and transaction testing for accurate monitoring of system and key transaction health without relying on third parties.

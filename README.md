# Login.gov Identity Reporting Rails

 Backend reporting and management for data warehouse.

 ### Purpose of the Rails App

The Reporting Rails application is developed to fulfill various essential functions. Its primary objectives include providing administrative access within the Virtual Private Cloud (VPC), executing background jobs, and overseeing data warehouse migrations in our source code.

### Administrative Access within the VPC

The Rails app will be designed to provide administrative access within our Virtual Private Cloud (VPC). This enables secure and controlled management of Redshift users (which needs to be through SQL access to the Redshift instance, and isn't through normal AWS admin APIs).

Managing Amazon Redshift database user management through a Rails application allows us to control who can access data warehouse using SQL commands. With this setup, we can create, edit, or delete users and roles as needed. It provides a user-friendly way to handle user access within the database while ensuring security and compliance.

### Running Background Jobs

It facilitates the execution of background jobs, automating repetitive tasks, and ensuring efficient resource utilization. These jobs can include data processing, report generation, and other automated tasks that need to run independently and asynchronously. The app provides a framework for defining, scheduling, and managing these background jobs.

### Managing Data Warehouse Migrations in Source Code

The app allows us to manage data warehouse migrations directly within the source code. This helps us keep our data schema up-to-date and maintain version control over database changes. The use of Rails migrations allows for smooth and reversible changes to the database schema.

### Other Functionalities

In addition to these primary purposes, our Rails app may include other functionalities and features that support our specific use cases and requirements.

## Getting Started

Refer to the [_Local Development_ documentation](./docs/local-development.md) to learn how to set up your environment for local development.

## Guides

- [The Contributing Guide](CONTRIBUTING.md) includes basic guidelines around pull requests, commit messages, and the code review process.
- [The Login.gov Handbook](https://handbook.login.gov/) describes organizational practices, including process runbooks and team structures.

## Documentation

- [Back-end Architecture](docs/backend.md)
- [Local Development](docs/local-development.md)
- [Security](docs/SECURITY.md)
- [Troubleshooting Local Development](docs/troubleshooting.md)

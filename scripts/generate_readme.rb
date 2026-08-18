require 'optparse'

class GenerateReadme
  attr_reader :docs_dir

  def initialize(docs_dir:)
    @docs_dir = docs_dir
  end

  # @return [String]
  def build
    <<~MARKDOWN
      ### Login.gov Identity Reporting Rails

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

      **This file is auto-generated**. Run `make README.md` to regenerate its contents.

      ## Getting Started

      Refer to the [_Local Development_ documentation](./docs/local-development.md) to learn how to set up your environment for local development.

      ## Guides

      - [The Contributing Guide](CONTRIBUTING.md) includes basic guidelines around pull requests, commit messages, and the code review process.
      - [The Login.gov Handbook](https://handbook.login.gov/) describes organizational practices, including process runbooks and team structures.

      ## Documentation

      #{table_of_contents}
    MARKDOWN
  end

  def table_of_contents
    docs_and_titles.map do |(title, path)|
      "- [#{title}](#{path})"
    end.join("\n")
  end

  # @return [Array<Array(String, String)>] a list of (title, path) tuples
  def docs_and_titles
    paths = Dir.glob("#{docs_dir}/**/*.md")
    (paths - gitignored(paths)).map do |path|
      title = guess_title(File.read(path))
      [title, path]
    end.sort_by(&:first)
  end

  # @return [Array<String>] the subset of paths excluded by gitignore rules
  def gitignored(paths)
    IO.popen(['git', 'check-ignore', '--stdin'], 'r+') do |io|
      io.puts(paths)
      io.close_write
      io.read.split("\n")
    end
  end

  # Guesses title from the first markdown heading in a file
  def guess_title(content)
    content.lines(chomp: true).each do |line|
      capture = line.match(/#+ (?<heading>.+)$/)
      break capture[:heading] if capture
    end || 'NO_TITLE'
  end

  def self.parse!(argv)
    options = {}

    parser = OptionParser.new do |opts|
      opts.banner = <<~TXT
        #{$PROGRAM_NAME} --docs-dir DOCS

        Generates a README.md by indexing into the given docs directory
      TXT

      opts.on('--docs-dir DIR', 'the directory to check for documents') do |dir|
        options[:docs_dir] = dir.chomp('/')
      end

      opts.on('--help') do
        puts opts
        exit 0
      end
    end

    parser.parse!(argv)

    if !options[:docs_dir]
      puts parser
      exit 1
    end

    options
  end
end

if __FILE__ == $PROGRAM_NAME
  options = GenerateReadme.parse!(ARGV)

  puts GenerateReadme.new(**options).build
end

# HttpOutput Plugin for Graylog

**Required Graylog version:** 2.0 and later

This Plugin has a very specific purpose:  Write the messages to an Http, indicated in the Plugin configuration parameters.

Getting started
---------------

This project is using Maven 3 and requires Java 8 or higher.

Installation
------------
[Download the plugin](https://github.com/sagarinpursue/graylog-http-plugin)

* Clone this repository.
* Run `mvn package` to build a JAR file.
* Optional: Run `mvn jdeb:jdeb` and `mvn rpm:rpm` to create a DEB and RPM package respectively.
* Copy generated JAR file in target directory to your Graylog plugin directory.
* Restart the Graylog.

The plugin directory is the `plugins/` folder relative from your `graylog-server` directory by default
and can be configured in your `graylog.conf` file.

Restart `graylog-server` and you are done.

Usage
-----

Once you have installed the plugin, you can configure an Output of type  com.plugin.HttpOutput, with this simple parameter:

output_api: API where the stream message is forwarded to.

Plugin Release
--------------

We are using the maven release plugin:

```
$ mvn release:prepare
[...]
$ mvn release:perform
```

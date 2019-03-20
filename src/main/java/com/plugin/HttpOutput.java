package com.plugin;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.graylog2.plugin.Message;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.outputs.MessageOutput;
import org.graylog2.plugin.streams.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import okhttp3.Call;
import okhttp3.Callback;
import java.util.concurrent.TimeUnit;

/**
 * This is the plugin. Your class should implement one of the existing plugin
 * interfaces. (i.e. AlarmCallback, MessageInput, MessageOutput)
 */
public class HttpOutput implements MessageOutput {

	private boolean shutdown;
	private String url;
	private static final String CK_OUTPUT_API = "output_api";
	/* connection timeout */
	private static final String CK_TIMEOUT = "timeout";
	private static final Logger LOG = LoggerFactory.getLogger(HttpOutput.class);

	@Inject
	public HttpOutput(@Assisted Stream stream, @Assisted Configuration conf) throws HttpOutputException {
		url = conf.getString(CK_OUTPUT_API);
		shutdown = false;
		LOG.info(" Http Output Plugin has been configured with the following parameters:");
		LOG.info(CK_OUTPUT_API + " : " + url);
		
		try {
            final URL urlTest = new URL(url);
        } catch (MalformedURLException e) {
        	LOG.info("Error in the given API", e);
            throw new HttpOutputException("Error while constructing the API.", e);
        }
	}

	@Override
	public boolean isRunning() {
		return !shutdown;
	}

	@Override
	public void stop() {
		shutdown = true;

	}

	@Override
	public void write(List<Message> msgs) throws Exception {
		for (Message msg : msgs) {
			writeBuffer(msg.getFields());
		}
	}

	@Override
	public void write(Message msg) throws Exception {
		if (shutdown) {
			return;
		}

		writeBuffer(msg.getFields());
	}

	public void writeBuffer(Map<String, Object> data) throws HttpOutputException {
		OkHttpClient client = new OkHttpClient.Builder()
			.connectTimeout(Integer.parseInt(CK_TIMEOUT), TimeUnit.SECONDS)
			.readTimeout(Integer.parseInt(CK_TIMEOUT), TimeUnit.SECONDS)
			.writeTimeout(Integer.parseInt(CK_TIMEOUT), TimeUnit.SECONDS)
			.build();
		
		Gson gson = new Gson();
		
		try {
			final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

			RequestBody body = RequestBody.create(JSON, gson.toJson(data));
			Request request = new Request.Builder().url(url).post(body).build();
			
			/*implement async request*/
			client.newCall(request).enqueue(new Callback()
			{
				/*failed to call*/
				@Override
				public void onFailure(Call call, IOException e) {
					LOG.info("HTTP output async request failed. ",e);
				}
				
				/*has response*/
				@Override
				public void onResponse(Call call, final Response response) throws IOException {
					if (!response.isSuccessful()) {
						/*do I need log here?*/
						response.close();
					} else {
						if(response.code() != 200){
							LOG.info("Unexpected HTTP response status:" + response.code() + ",body:" + response.body());
						}
						response.close();
					}
				}
			});
		} catch (Exception e) {
			LOG.info("Error while posting the stream data to the given API", e);
            		throw new HttpOutputException("Error while posting stream to HTTP.", e);
		}
	}

	public interface Factory extends MessageOutput.Factory<HttpOutput> {
		@Override
		HttpOutput create(Stream stream, Configuration configuration);

		@Override
		Config getConfig();

		@Override
		Descriptor getDescriptor();
	}

	public static class Descriptor extends MessageOutput.Descriptor {
		public Descriptor() {
			super("HttpOutput Output", false, "", "Forwards stream to HTTP.");
		}
	}

	public static class Config extends MessageOutput.Config {
		@Override
		public ConfigurationRequest getRequestedConfiguration() {
			final ConfigurationRequest configurationRequest = new ConfigurationRequest();
			
			configurationRequest.addField(new TextField(CK_OUTPUT_API, "API to forward the stream data.", "/",
					"HTTP address where the stream data to be sent.", ConfigurationField.Optional.NOT_OPTIONAL));
			configurationRequest.addField(new TextField(CK_TIMEOUT, "connection timeout","/",
					"timeout value for request", ConfigurationField.Optional.NOT_OPTIONAL));

			return configurationRequest;
		}
	}

	public class HttpOutputException extends Exception {

		private static final long serialVersionUID = -5301266791901423492L;

		public HttpOutputException(String msg) {
            super(msg);
        }

        public HttpOutputException(String msg, Throwable cause) {
            super(msg, cause);
        }

    }
}

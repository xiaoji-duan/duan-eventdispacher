package com.xiaoji.duan.aag;

import java.text.ParseException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.codec.digest.HmacAlgorithms;
import org.apache.commons.codec.digest.HmacUtils;
import org.thymeleaf.util.StringUtils;

import com.xiaoji.duan.aag.cron.CronTrigger;
import com.xiaoji.duan.aag.service.db.CreateTable;
import com.xiaoji.duan.aag.utils.Utils;

import io.vertx.amqpbridge.AmqpBridge;
import io.vertx.amqpbridge.AmqpBridgeOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;

/**
 * 
 * 事件分发 短应用
 * 
 * 事件短应用 发起 注册事件 动作短应用 发起 注册动作 任务短应用 发起 注册任务
 * 
 * Webhooks事件 支持GitHub 支持Fir.im
 * 
 * 任务包含 触发事件或者事件组合 触发动作 动作成功处理 动作失败处理
 * 
 * task_runat { eventId: 'eventId', filters: [ {name: 'event_output_name',
 * value: 'accept_value'} ], *alignTime: { client: [timestamp], server:
 * [timestamp] } }
 * 
 * task_runwith { url: 'url', payload: {...}, success: { url: 'url' }, error: {
 * url: 'url' } }
 * 
 * @author 席理加@效吉软件
 *
 */
public class MainVerticle extends AbstractVerticle {

	private SQLClient mySQLClient = null;
	private WebClient client = null;
	private AmqpBridge bridge = null;
	private AmqpBridge remote = null;

	private static Map<String, String> GitHubSecrets = new LinkedHashMap<String, String>();

	@Override
	public void start(Future<Void> startFuture) throws Exception {
		vertx.exceptionHandler(exception -> {
			error("Vertx exception caught.");
			connectRemoteStompServer();

		});

		client = WebClient.create(vertx);

		JsonObject mySQLClientConfig = new JsonObject().put("username", config().getString("mysql.username", "root"))
				.put("password", config().getString("mysql.password", "1234"))
				.put("host", config().getString("mysql.host", "mysql"))
				.put("database", config().getString("mysql.database", "duan"));
		mySQLClient = MySQLClient.createShared(vertx, mySQLClientConfig);

		// 初始化数据库
		CreateTable ct = new CreateTable();
		List<String> ddls = ct.getDdl();

		for (String ddl : ddls) {
			mySQLClient.update(ddl, update -> {

				if (update.succeeded()) {
					info("ddl");
				} else {
					update.cause().printStackTrace(System.out);
				}
			});
		}

		// 加载Github安全令牌
		mySQLClient.query("select * from aag_tasks;", ar -> this.loadsecrets(ar));

		bridge = AmqpBridge.create(vertx);

		bridge.endHandler(handler -> {
			connectStompServer();
		});
		connectStompServer();

		AmqpBridgeOptions remoteOption = new AmqpBridgeOptions();
		remoteOption.setReconnectAttempts(60);			// 重新连接尝试60次
		remoteOption.setReconnectInterval(60 * 1000);	// 每次尝试间隔1分钟
		
		remote = AmqpBridge.create(vertx, remoteOption);
//		remote.endHandler(handler -> {
//			connectRemoteStompServer();
//		});
		connectRemoteStompServer();

		Router router = Router.router(vertx);

		Set<HttpMethod> allowedMethods = new HashSet<HttpMethod>();
		allowedMethods.add(HttpMethod.OPTIONS);
		allowedMethods.add(HttpMethod.GET);
		allowedMethods.add(HttpMethod.POST);
		allowedMethods.add(HttpMethod.PUT);
		allowedMethods.add(HttpMethod.DELETE);
		allowedMethods.add(HttpMethod.CONNECT);
		allowedMethods.add(HttpMethod.PATCH);
		allowedMethods.add(HttpMethod.HEAD);
		allowedMethods.add(HttpMethod.TRACE);

		router.route()
				.handler(CorsHandler.create("*").allowedMethods(allowedMethods).allowedHeader("*")
						.allowedHeader("Content-Type").allowedHeader("lt").allowedHeader("pi").allowedHeader("pv")
						.allowedHeader("di").allowedHeader("latitude").allowedHeader("longitude").allowedHeader("dt")
						.allowedHeader("ai"));

		router.route("/aag/register/*").handler(BodyHandler.create());
		router.route("/aag/register/events").produces("application/json").handler(this::registerevents);
		router.route("/aag/register/tasks").produces("application/json").handler(this::registertasks);
		router.route("/aag/register/actions").produces("application/json").handler(this::registeractions);

		router.route("/aag/webhooks/*").handler(BodyHandler.create());
		router.route("/aag/webhooks/:webhookowner/:version/:observer").produces("application/json")
				.handler(this::webhook);

		vertx.createHttpServer().requestHandler(router::accept).listen(8080, http -> {
			if (http.succeeded()) {
				startFuture.complete();
				info("HTTP server started on http://localhost:8080");
			} else {
				startFuture.fail(http.cause());
			}
		});
	}

	private void connectStompServer() {
		bridge.start(config().getString("stomp.server.host", "sa-amq"), config().getInteger("stomp.server.port", 5672),
				res -> {
					if (res.failed()) {
						res.cause().printStackTrace();
						connectStompServer();
					} else {
						info("Stomp server connected.");

						// 初始化事件订阅
						mySQLClient.query("select * from aag_events;", ar -> this.subscribeevents(ar, bridge, false));

					}
				});
	}

	private void connectRemoteStompServer() {
		remote.start(config().getString("remote.server.host", "sa-amq"),
				config().getInteger("remote.server.port", 5672), res -> {
					if (res.failed()) {
//						res.cause().printStackTrace();
						connectRemoteStompServer();
					} else {
						info("Stomp remote server connected.");

						// 初始化事件订阅
						mySQLClient.query("select * from aag_events;", ar -> this.subscribeevents(ar, remote, true));

					}
				});
	}

	private void loadsecrets(AsyncResult<ResultSet> ar) {
		if (ar.succeeded()) {
			ResultSet rs = ar.result();

			List<JsonObject> registeredTasks = rs.getRows();

			debug(registeredTasks.size() + " tasks registered.");

			for (JsonObject task : registeredTasks) {
				String taskRunAt = task.getString("TASK_RUNAT");

				debug("TASK_RUNAT : " + taskRunAt);

				// 检查RunAt是否符合JSON格式
				try {
					JsonObject runAt = new JsonObject(taskRunAt);

					// 如果要求设置客户端ip，则进行设置，否则不设置
					if (runAt.containsKey("filters")) {
						debug("RUNAT has filters");

						JsonArray filters = runAt.getJsonArray("filters");
						if (filters != null && filters.size() > 0) {

							String secret = "";
							String observer = "";

							for (int i = 0; i < filters.size(); i++) {
								JsonObject json = filters.getJsonObject(i);
								debug(json.encode());

								if ("secret".equals(json.getString("name"))) {
									secret = json.getString("value", "");
								}

								if ("observer".equals(json.getString("name"))) {
									observer = json.getString("value", "");
								}
							}

							if (!StringUtils.isEmpty(secret) && !StringUtils.isEmpty(observer)) {
								GitHubSecrets.put(observer, secret);

								debug(observer + " <=> " + secret);
							}
						}
					}
				} catch (Exception e) {
					error("loadsecrets - " + e.getMessage());
					if (StringUtils.isEmpty(taskRunAt)) {
						taskRunAt = new JsonObject().encode();
					}
				}
			}
		} else {
			error("loadsecrets - " + ar.cause().getMessage());
			ar.cause().printStackTrace();
		}
	}

	private void registerevents(RoutingContext ctx) {
		debug(ctx.getBodyAsString());
		JsonObject body = ctx.getBodyAsJson();

		String saName = body.getString("saName");
		String saPrefix = body.getString("saPrefix");
		String eventId = body.getString("eventId");
		String eventType = body.getString("eventType");
		String eventName = body.getString("eventName");

		JsonArray params = new JsonArray();
		params.add(saPrefix);
		params.add(eventId);

		JsonArray insertparams = new JsonArray();
		insertparams.add(UUID.randomUUID().toString());
		insertparams.add(saName);
		insertparams.add(saPrefix);
		insertparams.add(eventId);
		insertparams.add(eventType);
		insertparams.add(eventName);

		JsonArray updateparams = new JsonArray();
		updateparams.add(saName);
		updateparams.add(eventType);
		updateparams.add(eventName);
		updateparams.add(saPrefix);
		updateparams.add(eventId);

		mySQLClient.queryWithParams("select * from aag_events where sa_prefix = ? and event_id = ?", params,
				handler -> this.ifexist(
						"insert into aag_events(unionid, sa_name, sa_prefix, event_id, event_type, event_name, create_time) values(?, ?, ?, ?, ?, ?, now());",
						insertparams,
						"update aag_events set sa_name = ?, event_type = ?, event_name = ? where sa_prefix = ? and event_id = ?;",
						updateparams, handler));

		JsonObject resp = new JsonObject();
		resp.put("code", "0");
		resp.put("message", "");
		resp.put("data", new JsonObject());

		ctx.response().putHeader("Content-Type", "application/json;charset=UTF-8").end(resp.encode());
	}

	private void registertasks(RoutingContext ctx) {
		JsonObject body = ctx.getBodyAsJson();

		String ipAddress = ctx.request().getHeader("x-forwarded-for");

		if (StringUtils.isEmpty(ipAddress)) {
			ipAddress = "222.64.177.189"; // 上海IP
		} else if (ipAddress.contains(",")) {
			String[] addresses = ipAddress.split(",");

			ipAddress = addresses[0].trim();
		}

		String saName = body.getString("saName");
		String saPrefix = body.getString("saPrefix");
		String taskId = body.getString("taskId");
		String taskType = body.getString("taskType");
		String taskName = body.getString("taskName");
		String taskRunAt = body.getString("taskRunAt");
		String taskRunWith = body.getString("taskRunWith");

		// 检查RunAt是否符合JSON格式
		try {
			JsonObject runAt = new JsonObject(taskRunAt);
			debug("TASK_RUNAT" + runAt);
			// 如果要求设置客户端ip，则进行设置，否则不设置
			if (runAt.containsKey("filters")) {
				debug("RUNAT has filters");

				JsonArray filters = runAt.getJsonArray("filters");
				if (filters != null && filters.size() > 0) {

					String secret = "";
					String observer = "";

					for (int i = 0; i < filters.size(); i++) {
						JsonObject json = filters.getJsonObject(i);
						debug(json.encode());
						if ("secret".equals(json.getString("name"))) {
							secret = json.getString("value", "");
						}

						if ("observer".equals(json.getString("name"))) {
							observer = json.getString("value", "");
						}
					}

					if (!StringUtils.isEmpty(secret) && !StringUtils.isEmpty(observer)) {
						GitHubSecrets.put(observer, secret);
						debug(observer + " <=> " + secret);
					}
				}
			}
		} catch (Exception e) {
			error("registertasks - " + e.getMessage());
			if (StringUtils.isEmpty(taskRunAt)) {
				taskRunAt = new JsonObject().encode();
			}
		}

		// 检查RunWith是否符合JSON格式
		try {
			JsonObject runWith = new JsonObject(taskRunWith);

			// 如果要求设置客户端ip，则进行设置，否则不设置
			if (runWith.containsKey("payload")) {
				if (runWith.getJsonObject("payload") != null
						&& !runWith.getJsonObject("payload").containsKey("clientip")) {
					runWith.getJsonObject("payload").put("clientip", ipAddress);
					taskRunWith = runWith.encode();
				}
			}
		} catch (Exception e) {
			error("registertasks - " + e.getMessage());
			if (StringUtils.isEmpty(taskRunWith)) {
				taskRunWith = new JsonObject().encode();
			}
		}

		JsonArray params = new JsonArray();
		params.add(saPrefix);
		params.add(taskId);

		JsonArray insertparams = new JsonArray();
		insertparams.add(UUID.randomUUID().toString());
		insertparams.add(saName);
		insertparams.add(saPrefix);
		insertparams.add(taskId);
		insertparams.add(taskType);
		insertparams.add(taskName);
		insertparams.add(taskRunAt);
		insertparams.add(taskRunWith);

		JsonArray updateparams = new JsonArray();
		updateparams.add(saName);
		updateparams.add(taskType);
		updateparams.add(taskName);
		updateparams.add(taskRunAt);
		updateparams.add(taskRunWith);
		updateparams.add(saPrefix);
		updateparams.add(taskId);

		info("refresh task with " + params.encode());
		mySQLClient.queryWithParams("select * from aag_tasks where sa_prefix = ? and task_id = ?;", params,
				handler -> this.ifexist(
						"insert into aag_tasks(unionid, sa_name, sa_prefix, task_id, task_type, task_name, task_runat, task_runwith, create_time) values(?, ?, ?, ?, ?, ?, ?, ?, now())",
						insertparams,
						"update aag_tasks set sa_name = ?, task_type = ?, task_name = ?, task_runat = ?, task_runwith = ? where sa_prefix = ? and task_id = ?",
						updateparams, handler));

		JsonObject resp = new JsonObject();
		resp.put("code", "0");
		resp.put("message", "");
		resp.put("data", new JsonObject());

		ctx.response().putHeader("Content-Type", "application/json;charset=UTF-8").end(resp.encode());
	}

	private void webhook(RoutingContext ctx) {
		String owner = ctx.pathParam("webhookowner");
		String version = ctx.pathParam("version");
		String observer = ctx.pathParam("observer");

		HttpServerRequest req = ctx.request();

		try {
			if ("github".equals(owner) && "v3".equals(version)) {
				String sign = req.getHeader("X-Hub-Signature");
				String sb = ctx.getBodyAsString();

				JsonObject message = null;

				if (!StringUtils.isEmpty(sign) && !StringUtils.isEmpty(sb)) {
					String userSecret = GitHubSecrets.getOrDefault(observer,
							config().getString("user.secret", "N2ZxMDdlMzhlY2Yw-7fa07e38ecf0831"));
					debug(observer + " <-> " + userSecret);

					HmacUtils hm1 = new HmacUtils(HmacAlgorithms.HMAC_SHA_1, userSecret);
					String signature = hm1.hmacHex(sb);

					if (!StringUtils.isEmpty(signature) && sign.equals("sha1=" + signature)) {
						String trigger = "aag" + "_" + "WEBHOOK_GITHUB".toLowerCase();

						message = ctx.getBodyAsJson();

						// 增加可过滤条件
						String repository = (message != null
								? message.getJsonObject("repository", new JsonObject()).getString("full_name", "")
								: "");

						MessageProducer<JsonObject> producer = bridge.createProducer(trigger);

						long triggerTime = System.currentTimeMillis();

						JsonObject output = new JsonObject().put("webhook", "github").put("observer", observer)
								.put("secret", userSecret).put("payload", message).put("repository", repository);

						JsonObject body = new JsonObject().put("context",
								new JsonObject().put("trigger_time", triggerTime)
										.put("trigger_time_fmt", Utils.getFormattedTime(triggerTime))
										.put("output", output));
						debug("Event [" + trigger + "] triggered.");

						producer.send(new JsonObject().put("body", body));
						producer.end();
					}
				} else {
					// ctx.response().setStatusCode(403).end("Illegal access,
					// forbbiden!");
				}

				debug(
						"Github webhook launched with " + ((message == null) ? "empty" : message.encodePrettily()));
			}

			if ("fir.im".equals(owner) && "v3".equals(version)) {
				String trigger = "aag" + "_" + "WEBHOOK_FIR.IM".toLowerCase();

				JsonObject message = new JsonObject();

				Iterator<Entry<String, String>> params = ctx.request().params().iterator();

				while (params.hasNext()) {
					Entry<String, String> entry = params.next();

					message.put(entry.getKey(), entry.getValue());
				}

				// 增加可过滤条件
				String link = (message != null ? message.getString("link", "") : "");

				MessageProducer<JsonObject> producer = bridge.createProducer(trigger);

				long triggerTime = System.currentTimeMillis();

				JsonObject output = new JsonObject().put("webhook", "fir.im").put("observer", observer)
						.put("payload", message).put("link", link);

				JsonObject body = new JsonObject().put("context", new JsonObject().put("trigger_time", triggerTime)
						.put("trigger_time_fmt", Utils.getFormattedTime(triggerTime)).put("output", output));
				debug("Event [" + trigger + "] triggered.");

				producer.send(new JsonObject().put("body", body));
				producer.end();

				debug(
						"Fir.im webhook launched with " + message == null ? "empty" : message.encodePrettily());
			}
		} catch (Exception e) {
			error("webhook - " + e.getMessage());
			e.printStackTrace();
		} finally {
			ctx.response().end("ok");
		}
	}

	private void registeractions(RoutingContext ctx) {
		JsonObject body = ctx.getBodyAsJson();

		String saName = body.getString("saName");
		String saPrefix = body.getString("saPrefix");
		String actionId = body.getString("actionId");
		String actionType = body.getString("actionType");
		String actionName = body.getString("actionName");
		String actionRunWith = body.getString("actionRunWith");

		JsonArray params = new JsonArray();
		params.add(saPrefix);
		params.add(actionId);

		JsonArray insertparams = new JsonArray();
		insertparams.add(UUID.randomUUID().toString());
		insertparams.add(saName);
		insertparams.add(saPrefix);
		insertparams.add(actionId);
		insertparams.add(actionType);
		insertparams.add(actionName);
		insertparams.add(actionRunWith);

		JsonArray updateparams = new JsonArray();
		updateparams.add(saName);
		updateparams.add(actionType);
		updateparams.add(actionName);
		updateparams.add(actionRunWith);
		updateparams.add(saPrefix);
		updateparams.add(actionId);

		info("refresh action with " + params.encode());
		mySQLClient.queryWithParams("select * from aag_actions where sa_prefix = ? and action_id = ?", params,
				handler -> this.ifexist(
						"insert into aag_actions(unionid, sa_name, sa_prefix, action_id, action_type, action_name, action_runwith, create_time) values(?, ?, ?, ?, ?, ?, ?, now());",
						insertparams,
						"update aag_actions set sa_name = ?, action_type = ?, action_name = ?, action_runwith = ? where sa_prefix = ? and action_id = ?;",
						updateparams, handler));

		JsonObject resp = new JsonObject();
		resp.put("code", "0");
		resp.put("message", "");
		resp.put("data", new JsonObject());

		ctx.response().putHeader("Content-Type", "application/json;charset=UTF-8").end(resp.encode());
	}

	private void ifexist(String insert, JsonArray insertparams, String update, JsonArray updateparams,
			AsyncResult<ResultSet> ar) {
		if (ar.succeeded()) {
			ResultSet rs = ar.result();

			if (rs.getNumRows() > 0) {
				debug("update task with " + updateparams.encode());
				// 存在记录 更新
				mySQLClient.updateWithParams(update, updateparams, handler -> {
				});
			} else {
				debug("insert task with " + insertparams.encode());
				// 不存在记录 插入
				mySQLClient.updateWithParams(insert, insertparams, handler -> {
				});
			}

		} else {
			error("ifexist - " + ar.cause().getMessage());
			ar.cause().printStackTrace();
		}
	}

	private void subscribeevents(AsyncResult<ResultSet> ar, AmqpBridge bridge, Boolean isRemote) {
		if (ar.succeeded()) {
			ResultSet rs = ar.result();

			List<JsonObject> registeredEvents = rs.getRows();

			info(registeredEvents.size() + " events registered.");

			for (JsonObject regEvent : registeredEvents) {
				this.subscribe(regEvent, bridge, isRemote);
			}
		} else {
			error("subscribeevents - " + ar.cause().getMessage());
			ar.cause().printStackTrace();
		}
	}

	private void subscribe(JsonObject event, AmqpBridge bridge, Boolean isRemote) {
		debug(event.encode());

		String saPrefix = event.getString("SA_PREFIX");
		String eventId = event.getString("EVENT_ID");
		String eventType = event.getString("EVENT_TYPE");

		String trigger = saPrefix.toLowerCase() + "_" + eventId.toLowerCase();

		if (isRemote) {
			if ("QUARTZ.1M".equals(eventType) || "QUARTZ.5M".equals(eventType) || "QUARTZ.1H".equals(eventType)) {
				MessageConsumer<JsonObject> consumer = bridge.createConsumer(trigger);
				debug("Event [" + trigger + "] subscribed.");
				consumer.exceptionHandler(exception -> {
					error("MessageConsumer exception caught.");
					connectRemoteStompServer();
				});
				consumer.handler(vertxMsg -> this.eventTriggered(event, vertxMsg));
			}
		} else {
			if (!"QUARTZ.1M".equals(eventType) && !"QUARTZ.5M".equals(eventType) && !"QUARTZ.1H".equals(eventType)) {
				MessageConsumer<JsonObject> consumer = bridge.createConsumer(trigger);
				debug("Event [" + trigger + "] subscribed.");
				consumer.handler(vertxMsg -> this.eventTriggered(event, vertxMsg));
			}
		}

		if (config().getBoolean("cron.trigger", Boolean.FALSE) && "QUARTZ.1M".equals(eventType)) {
			try {
				CronTrigger cron1m = new CronTrigger(vertx, "0 */1 * * * ?");

				cron1m.schedule(handler -> {
					MessageProducer<JsonObject> producer = bridge.createProducer(trigger);

					long triggerTime = System.currentTimeMillis();

					JsonObject output = new JsonObject().put("yyyy", Utils.getTimeFormat(triggerTime, "yyyy"))
							.put("MM", Utils.getTimeFormat(triggerTime, "MM"))
							.put("dd", Utils.getTimeFormat(triggerTime, "dd"))
							.put("HH", Utils.getTimeFormat(triggerTime, "HH"))
							.put("mm", Utils.getTimeFormat(triggerTime, "mm"))
							.put("ss", Utils.getTimeFormat(triggerTime, "ss"));

					JsonObject body = new JsonObject().put("context", new JsonObject().put("trigger_time", triggerTime)
							.put("trigger_time_fmt", Utils.getFormattedTime(triggerTime)).put("output", output));
					debug("Event [" + trigger + "] triggered.");

					producer.send(new JsonObject().put("body", body));
					producer.end();
				});

				debug("Event [" + trigger + "] scheduled.");
			} catch (ParseException e) {
				error("subscribe - " + e.getMessage());
				e.printStackTrace();
			}
		}

		if (config().getBoolean("cron.trigger", Boolean.FALSE) && "QUARTZ.5M".equals(eventType)) {
			try {
				CronTrigger cron5m = new CronTrigger(vertx, "0 0/5 * * * ?");

				cron5m.schedule(handler -> {
					MessageProducer<JsonObject> producer = bridge.createProducer(trigger);

					long triggerTime = System.currentTimeMillis();

					JsonObject output = new JsonObject().put("yyyy", Utils.getTimeFormat(triggerTime, "yyyy"))
							.put("MM", Utils.getTimeFormat(triggerTime, "MM"))
							.put("dd", Utils.getTimeFormat(triggerTime, "dd"))
							.put("HH", Utils.getTimeFormat(triggerTime, "HH"))
							.put("mm", Utils.getTimeFormat(triggerTime, "mm"))
							.put("ss", Utils.getTimeFormat(triggerTime, "ss"));

					JsonObject body = new JsonObject().put("context", new JsonObject().put("trigger_time", triggerTime)
							.put("trigger_time_fmt", Utils.getFormattedTime(triggerTime)).put("output", output));
					debug("Event [" + trigger + "] triggered.");

					producer.send(new JsonObject().put("body", body));
					producer.end();
				});

				debug("Event [" + trigger + "] scheduled.");
			} catch (ParseException e) {
				error("subscribe - " + e.getMessage());
				e.printStackTrace();
			}
		}

		if (config().getBoolean("cron.trigger", Boolean.FALSE) && "QUARTZ.1H".equals(eventType)) {
			try {
				CronTrigger cron1h = new CronTrigger(vertx, "0 0 0/1 * * ?");

				cron1h.schedule(handler -> {
					MessageProducer<JsonObject> producer = bridge.createProducer(trigger);

					long triggerTime = System.currentTimeMillis();

					JsonObject output = new JsonObject().put("yyyy", Utils.getTimeFormat(triggerTime, "yyyy"))
							.put("MM", Utils.getTimeFormat(triggerTime, "MM"))
							.put("dd", Utils.getTimeFormat(triggerTime, "dd"))
							.put("HH", Utils.getTimeFormat(triggerTime, "HH"))
							.put("mm", Utils.getTimeFormat(triggerTime, "mm"))
							.put("ss", Utils.getTimeFormat(triggerTime, "ss"));

					JsonObject body = new JsonObject().put("context", new JsonObject().put("trigger_time", triggerTime)
							.put("trigger_time_fmt", Utils.getFormattedTime(triggerTime)).put("output", output));
					debug("Event [" + trigger + "] triggered.");

					producer.send(new JsonObject().put("body", body));
					producer.end();
				});

				debug("Event [" + trigger + "] scheduled.");
			} catch (ParseException e) {
				error("subscribe - " + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	private void eventTriggered(JsonObject event, Message<JsonObject> received) {
		String eventId = event.getString("EVENT_ID");

		if (!(received.body().getValue("body") instanceof JsonObject)) {
			error("Message content is not JsonObject, process stopped.");
			return;
		}

		JsonObject message = received.body().getJsonObject("body");
		debug(eventId + " : " + message.encode());

		// 查询任务, 分发事件
		mySQLClient.query("select * from aag_tasks where task_runat like '%" + eventId + "%' and task_runat not like '%active%false%' order by create_time desc;",
				ar -> this.dispatchEvents(event, message.getJsonObject("context"), ar));
	}

	private void dispatchEvents(JsonObject event, JsonObject message, AsyncResult<ResultSet> ar) {
		if (ar.succeeded()) {
			JsonObject eventcopy = event.copy();
			ResultSet rs = ar.result();

			List<JsonObject> tasks = new LinkedList<JsonObject>();
			rs.getRows().forEach(tasks::add);;
			
			// 用户日志跟踪
			String execId = UUID.randomUUID().toString();
			String eventId = event.getString("EVENT_ID");

			vertx.executeBlocking(future -> {
				Integer index = 0;
				Integer total = 0;

				total = tasks.size();
				
				error("[" + eventId + "]" + "[" + execId + "] " + total + " tasks registered.");

				for (JsonObject task : tasks) {
					index++;
					
					debug("[" + eventId + "]" + "[" + execId + "] " + "task " + index + "/" + total + " .");
					try {
						if (accept(task, eventcopy, message)) {
							error("[" + eventId + "]" + "[" + execId + "] " + index + "/" + total + " task " + task.getString("TASK_NAME") + " accepted.");
							JsonObject runwith = new JsonObject(task.getString("TASK_RUNWITH"));
	
							String taskUrl = runwith.getString("url");
							JsonObject payload = runwith.getJsonObject("payload", new JsonObject());
	
							HttpRequest<Buffer> request = client.postAbs(taskUrl);
	
							payload.put("event", message);
	
							debug("[" + eventId + "]" + "[" + execId + "] " + "task " + task.getString("TASK_NAME") + " run with " + taskUrl);
							debug("[" + eventId + "]" + "[" + execId + "] " + "task " + task.getString("TASK_NAME") + " run payload " + payload.encode());
	
							request.sendJsonObject(payload, handler -> this.callback(task, eventcopy, message, handler));
						} else {
							debug("[" + eventId + "]" + "[" + execId + "] " + index + "/" + total + " task " + task.getString("TASK_NAME") + " unaccepted.");
						}
					} catch (Exception e) {
						error("dispatchEvents - " + e.getMessage());
						e.printStackTrace();
					}
				}
				
				future.complete(new JsonObject().put("index", index).put("total", total));
			}, complete -> {
				if (complete.succeeded()) {
					JsonObject result = (JsonObject) complete.result();

					Integer index = result.getInteger("index", 0);
					Integer total = result.getInteger("total", 0);
					
					error("[" + eventId + "]" + "[" + execId + "] " + index + "/" + total + " completed.");
				} else {
					error("[" + eventId + "]" + "[" + execId + "] completed with error.");
				}
				
			});
		} else {
			error("dispatchEvents - " + ar.cause().getMessage());
			ar.cause().printStackTrace();
		}
	}

	private boolean accept(JsonObject task, JsonObject event, JsonObject message) {
		String srunat = task.getString("TASK_RUNAT");

		debug("event output " + message.encode());
		debug("event runat " + srunat);

		JsonObject runat = new JsonObject(srunat);

		String taskEventId = runat.getString("eventId", "");
		JsonArray filters = runat.getJsonArray("filters", new JsonArray());
		String eventId = event.getString("EVENT_ID", "");

		if (Utils.isEmpty(taskEventId) || !taskEventId.equals(eventId)) {
			return false;
		}

		for (int i = 0; i < filters.size(); i++) {
			Object test = filters.getValue(i);

			if (test instanceof JsonObject) {
				JsonObject filter = filters.getJsonObject(i);

				String name = filter.getString("name", "");
				Object valuetest = filter.getValue("value");

				String value = String.valueOf(valuetest);

				if (Utils.isEmpty(name) || Utils.isEmpty(value)) {
					continue;
				}

				JsonObject output = message.getJsonObject("output", new JsonObject());

				if (output.isEmpty()) {
					return false;
				}

				if (!value.equals(output.getString(name))) {
					return false;
				}
			} else {
				debug("skipped filter " + test);
			}
		}

		return true;
	}

	private void callback(JsonObject task, JsonObject event, JsonObject message, AsyncResult<HttpResponse<Buffer>> ar) {

		String taskSuccessUrl = "";
		String taskErrorUrl = "";

		// 成功或者失败处理时，任务为空
		if (task != null) {
			JsonObject runwith = new JsonObject(task.getString("TASK_RUNWITH"));

			taskSuccessUrl = (runwith.getJsonObject("success") == null ? new JsonObject()
					: runwith.getJsonObject("success")).getString("url");
			taskErrorUrl = (runwith.getJsonObject("error") == null ? new JsonObject() : runwith.getJsonObject("error"))
					.getString("url");
		}

		if (ar.succeeded()) {
			HttpResponse<Buffer> response = ar.result();

			int statusCode = response.statusCode();

			if (statusCode == 200) {

				if (!Utils.isEmpty(taskSuccessUrl)) {
					JsonObject resp = response.bodyAsJsonObject();

					JsonObject data = resp.getJsonObject("data");

					HttpRequest<Buffer> request = client.getAbs(taskSuccessUrl);

					JsonObject body = new JsonObject();
					body.put("data", data);

					request.sendJsonObject(body, handler -> this.callback(null, event, message, handler));
				}
			} else {
				// 调用异常处理
				if (!Utils.isEmpty(taskErrorUrl)) {
					HttpRequest<Buffer> request = client.getAbs(taskErrorUrl);

					JsonObject body = new JsonObject();
					body.put("data", message);

					request.sendJsonObject(body, handler -> this.callback(null, event, message, handler));
				}
			}
		} else {
			error("callback - " + ar.cause().getMessage());
			ar.cause().printStackTrace();

			// 调用异常处理
			if (!Utils.isEmpty(taskErrorUrl)) {
				HttpRequest<Buffer> request = client.getAbs(taskErrorUrl);

				JsonObject body = new JsonObject();
				body.put("data", message);

				request.sendJsonObject(body, handler -> this.callback(null, event, message, handler));
			}
		}
	}

	private void info(String log) {
		if (config().getBoolean("log.info", Boolean.FALSE)) {
			System.out.println(log);
		}
	}

	private void debug(String log) {
		if (config().getBoolean("log.debug", Boolean.FALSE)) {
			System.out.println(log);
		}
	}

	private void error(String log) {
		if (config().getBoolean("log.error", Boolean.TRUE)) {
			System.out.println(log);
		}
	}
}

package com.xiaoji.duan.aag;

import java.text.ParseException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
 * Webhooks事件
 * 支持GitHub
 * 支持Fir.im
 * 
 * 任务包含 触发事件或者事件组合 触发动作 动作成功处理 动作失败处理
 * 
 * task_runat {
 *   eventId: 'eventId',
 *   filters: [
 *   	{name: 'event_output_name', value: 'accept_value'}
 *   ]
 * }
 * 
 * task_runwith {
 *   url: 'url',
 *   payload: {...},
 *   success: {
 *     url: 'url'
 *   },
 *   error: {
 *     url: 'url'
 *   }
 * }
 * 
 * @author 席理加@效吉软件
 *
 */
public class MainVerticle extends AbstractVerticle {

	private SQLClient mySQLClient = null;
	private WebClient client = null;
	private AmqpBridge bridge = null;
	private static Map<String, String> GitHubSecrets = new LinkedHashMap<String, String>();

	@Override
	public void start(Future<Void> startFuture) throws Exception {
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
					System.out.println("ddl");
				} else {
					update.cause().printStackTrace(System.out);
				}
			});
		}

		//加载Github安全令牌
		mySQLClient.query("select * from aag_tasks;", ar -> this.loadsecrets(ar));

		bridge = AmqpBridge.create(vertx);

		bridge.endHandler(handler -> {
			connectStompServer();
		});
		connectStompServer();

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

		router.route().handler(CorsHandler.create("*")
				.allowedMethods(allowedMethods)
				.allowedHeader("*")
				.allowedHeader("Content-Type")
				.allowedHeader("lt")
				.allowedHeader("pi")
				.allowedHeader("pv")
				.allowedHeader("di")
				.allowedHeader("dt")
				.allowedHeader("ai"));

		router.route("/aag/register/*").handler(BodyHandler.create());
		router.route("/aag/register/events").produces("application/json").handler(this::registerevents);
		router.route("/aag/register/tasks").produces("application/json").handler(this::registertasks);
		router.route("/aag/register/actions").produces("application/json").handler(this::registeractions);

		router.route("/aag/webhooks/*").handler(BodyHandler.create());
		router.route("/aag/webhooks/:webhookowner/:version/:observer").produces("application/json").handler(this::webhook);

		vertx.createHttpServer().requestHandler(router::accept).listen(8080, http -> {
			if (http.succeeded()) {
				startFuture.complete();
				System.out.println("HTTP server started on http://localhost:8080");
			} else {
				startFuture.fail(http.cause());
			}
		});
	}
	
	private void connectStompServer() {
		bridge.start(config().getString("stomp.server.host", "sa-amq"),
				config().getInteger("stomp.server.port", 5672), res -> {
					if (res.failed()) {
						res.cause().printStackTrace();
						connectStompServer();
					} else {
						System.out.println("Stomp server connected.");

						// 初始化事件订阅
						mySQLClient.query("select * from aag_events;", ar -> this.subscribeevents(ar));
						
					}
				});
	}
	
	private void loadsecrets(AsyncResult<ResultSet> ar) {
		if (ar.succeeded()) {
			ResultSet rs = ar.result();
			
			List<JsonObject> registeredTasks = rs.getRows();
			
			System.out.println(registeredTasks.size() + " tasks registered.");
			
			for (JsonObject task : registeredTasks) {
				String taskRunAt = task.getString("TASK_RUNAT");
				System.out.println("TASK_RUNAT : " + taskRunAt);
				//检查RunAt是否符合JSON格式
				try {
					JsonObject runAt = new JsonObject(taskRunAt);
					
					//如果要求设置客户端ip，则进行设置，否则不设置
					if (runAt.containsKey("filters")) {
						System.out.println("RUNAT has filters");
						JsonArray filters = runAt.getJsonArray("filters");
						if (filters != null && filters.size() > 0) {

							String secret = "";
							String observer = "";

							for (int i = 0; i < filters.size(); i++) {
								JsonObject json = filters.getJsonObject(i);
								System.out.println(json.encode());

								if ("secret".equals(json.getString("name"))) {
									secret = json.getString("value", "");
								}

								if ("observer".equals(json.getString("name"))) {
									observer = json.getString("value", "");
								}
							}
							
							if (!StringUtils.isEmpty(secret) && !StringUtils.isEmpty(observer)) {
								GitHubSecrets.put(observer, secret);
								System.out.println(observer + " <=> " + secret);
							}
						}
					}
				} catch (Exception e) {
					if (StringUtils.isEmpty(taskRunAt)) {
						taskRunAt = new JsonObject().encode();
					}
				}
			}
		} else {
			ar.cause().printStackTrace();
		}
	}
	
	private void registerevents(RoutingContext ctx) {
		System.out.println(ctx.getBodyAsString());
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

		mySQLClient.queryWithParams(
				"select * from aag_events where sa_prefix = ? and event_id = ?",
				params,
				handler -> this.ifexist(
						"insert into aag_events(unionid, sa_name, sa_prefix, event_id, event_type, event_name, create_time) values(?, ?, ?, ?, ?, ?, now());",
						insertparams,
						"update aag_events set sa_name = ?, event_type = ?, event_name = ? where sa_prefix = ? and event_id = ?;",
						updateparams,
						handler));
		
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
			ipAddress = "222.64.177.189";	//上海IP
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
		
		//检查RunAt是否符合JSON格式
		try {
			JsonObject runAt = new JsonObject(taskRunAt);
			System.out.println("TASK_RUNAT" + runAt);
			//如果要求设置客户端ip，则进行设置，否则不设置
			if (runAt.containsKey("filters")) {
				System.out.println("RUNAT has filters");

				JsonArray filters = runAt.getJsonArray("filters");
				if (filters != null && filters.size() > 0) {
					
					String secret = "";
					String observer = "";

					for (int i = 0; i < filters.size(); i++) {
						JsonObject json = filters.getJsonObject(i);
						System.out.println(json.encode());
						if ("secret".equals(json.getString("name"))) {
							secret = json.getString("value", "");
						}

						if ("observer".equals(json.getString("name"))) {
							observer = json.getString("value", "");
						}
					}
					
					if (!StringUtils.isEmpty(secret) && !StringUtils.isEmpty(observer)) {
						GitHubSecrets.put(observer, secret);
						System.out.println(observer + " <=> " + secret);
					}
				}
			}
		} catch (Exception e) {
			if (StringUtils.isEmpty(taskRunAt)) {
				taskRunAt = new JsonObject().encode();
			}
		}
		
		//检查RunWith是否符合JSON格式
		try {
			JsonObject runWith = new JsonObject(taskRunWith);
			
			//如果要求设置客户端ip，则进行设置，否则不设置
			if (runWith.containsKey("payload")) {
				if (runWith.getJsonObject("payload") != null && !runWith.getJsonObject("payload").containsKey("clientip")) {
					runWith.getJsonObject("payload").put("clientip", ipAddress);
					taskRunWith = runWith.encode();
				}
			}
		} catch (Exception e) {
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

		System.out.println("refresh task with " + params.encode());
		mySQLClient.queryWithParams(
				"select * from aag_tasks where sa_prefix = ? and task_id = ?;",
				params,
				handler -> this.ifexist(
						"insert into aag_tasks(unionid, sa_name, sa_prefix, task_id, task_type, task_name, task_runat, task_runwith, create_time) values(?, ?, ?, ?, ?, ?, ?, ?, now())",
						insertparams,
						"update aag_tasks set sa_name = ?, task_type = ?, task_name = ?, task_runat = ?, task_runwith = ? where sa_prefix = ? and task_id = ?",
						updateparams,
						handler));

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
					String userSecret = GitHubSecrets.getOrDefault(observer, config().getString("user.secret", "N2ZxMDdlMzhlY2Yw-7fa07e38ecf0831"));
					System.out.println(observer + " <-> " + userSecret);
					
					HmacUtils hm1 = new HmacUtils(HmacAlgorithms.HMAC_SHA_1, userSecret);
					String signature = hm1.hmacHex(sb);
		
					if (!StringUtils.isEmpty(signature) && sign.equals("sha1=" + signature)) {
						String trigger = "aag" + "_" + "WEBHOOK_GITHUB".toLowerCase();

						message = ctx.getBodyAsJson();
						
						//增加可过滤条件
						String repository = (message != null? message
								.getJsonObject("repository", new JsonObject())
								.getString("full_name", "") : "");
						
						MessageProducer<JsonObject> producer = bridge.createProducer(trigger);

						long triggerTime = System.currentTimeMillis();
						
						JsonObject output = new JsonObject()
								.put("webhook", "github")
								.put("observer", observer)
								.put("secret", userSecret)
								.put("payload", message)
								.put("repository", repository);

						JsonObject body = new JsonObject().put("context", new JsonObject()
								.put("trigger_time", triggerTime)
								.put("trigger_time_fmt", Utils.getFormattedTime(triggerTime))
								.put("output", output));
						System.out.println("Event [" + trigger + "] triggered.");

						producer.send(new JsonObject().put("body", body));
					}
				} else {
					//ctx.response().setStatusCode(403).end("Illegal access, forbbiden!");
				}
		
				System.out.println("Github webhook launched with " + ((message == null)? "empty" : message.encodePrettily()));
			}
			
			if ("fir.im".equals(owner) && "v3".equals(version)) {
				String trigger = "aag" + "_" + "WEBHOOK_FIR.IM".toLowerCase();

				JsonObject message = new JsonObject();
				
				Iterator<Entry<String, String>> params = ctx.request().params().iterator();
				
				while(params.hasNext()) {
					Entry<String, String> entry = params.next();

					message.put(entry.getKey(), entry.getValue());
				}

				//增加可过滤条件
				String link = (message != null? message.getString("link", "") : "");
				
				MessageProducer<JsonObject> producer = bridge.createProducer(trigger);

				long triggerTime = System.currentTimeMillis();
				
				JsonObject output = new JsonObject()
						.put("webhook", "fir.im")
						.put("observer", observer)
						.put("payload", message)
						.put("link", link);

				JsonObject body = new JsonObject().put("context", new JsonObject()
						.put("trigger_time", triggerTime)
						.put("trigger_time_fmt", Utils.getFormattedTime(triggerTime))
						.put("output", output));
				System.out.println("Event [" + trigger + "] triggered.");

				producer.send(new JsonObject().put("body", body));
				
				System.out.println("Fir.im webhook launched with " + message == null? "empty" : message.encodePrettily());
			}
		} catch (Exception e) {
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

		System.out.println("refresh action with " + params.encode());
		mySQLClient.queryWithParams(
				"select * from aag_actions where sa_prefix = ? and action_id = ?",
				params,
				handler -> this.ifexist(
						"insert into aag_actions(unionid, sa_name, sa_prefix, action_id, action_type, action_name, action_runwith, create_time) values(?, ?, ?, ?, ?, ?, ?, now());",
						insertparams,
						"update aag_actions set sa_name = ?, action_type = ?, action_name = ?, action_runwith = ? where sa_prefix = ? and action_id = ?;",
						updateparams,
						handler));
		
		JsonObject resp = new JsonObject();
		resp.put("code", "0");
		resp.put("message", "");
		resp.put("data", new JsonObject());
		
		ctx.response().putHeader("Content-Type", "application/json;charset=UTF-8").end(resp.encode());
	}
	
	private void ifexist(String insert, JsonArray insertparams, String update, JsonArray updateparams, AsyncResult<ResultSet> ar) {
		if (ar.succeeded()) {
			ResultSet rs = ar.result();
			
			if (rs.getNumRows() > 0) {
				System.out.println("update task with " + updateparams.encode());
				// 存在记录 更新
				mySQLClient.updateWithParams(update, updateparams, handler -> {});
			} else {
				System.out.println("insert task with " + insertparams.encode());
				// 不存在记录 插入
				mySQLClient.updateWithParams(insert, insertparams, handler -> {});
			}
			
		} else {
			ar.cause().printStackTrace();
		}
	}

	private void subscribeevents(AsyncResult<ResultSet> ar) {
		if (ar.succeeded()) {
			ResultSet rs = ar.result();
			
			List<JsonObject> registeredEvents = rs.getRows();
			
			System.out.println(registeredEvents.size() + " events registered.");
			
			for (JsonObject regEvent : registeredEvents) {
				this.subscribe(regEvent);
			}
		} else {
			ar.cause().printStackTrace();
		}
	}
	
	private void subscribe(JsonObject event) {
		System.out.println(event.encode());
		
		String saPrefix = event.getString("SA_PREFIX");
		String eventId = event.getString("EVENT_ID");
		String eventType = event.getString("EVENT_TYPE");
		
		String trigger = saPrefix.toLowerCase() + "_" + eventId.toLowerCase();
		
		MessageConsumer<JsonObject> consumer = bridge.createConsumer(trigger);
		System.out.println("Event [" + trigger + "] subscribed.");
		consumer.handler(vertxMsg -> this.eventTriggered(event, vertxMsg));

		if ("QUARTZ.1M".equals(eventType)) {
			try {
				CronTrigger cron1m = new CronTrigger(vertx, "0 */1 * * * ?");
				
				cron1m.schedule(handler -> {
					MessageProducer<JsonObject> producer = bridge.createProducer(trigger);

					long triggerTime = System.currentTimeMillis();
					
					JsonObject output = new JsonObject()
							.put("yyyy", Utils.getTimeFormat(triggerTime, "yyyy"))
							.put("MM", Utils.getTimeFormat(triggerTime, "MM"))
							.put("dd", Utils.getTimeFormat(triggerTime, "dd"))
							.put("HH", Utils.getTimeFormat(triggerTime, "HH"))
							.put("mm", Utils.getTimeFormat(triggerTime, "mm"))
							.put("ss", Utils.getTimeFormat(triggerTime, "ss"));
					
					JsonObject body = new JsonObject().put("context", new JsonObject()
							.put("trigger_time", triggerTime)
							.put("trigger_time_fmt", Utils.getFormattedTime(triggerTime))
							.put("output", output));
					System.out.println("Event [" + trigger + "] triggered.");

					producer.send(new JsonObject().put("body", body));

				});

				System.out.println("Event [" + trigger + "] scheduled.");
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		
		if ("QUARTZ.5M".equals(eventType)) {
			try {
				CronTrigger cron5m = new CronTrigger(vertx, "0 0/5 * * * ?");
				
				cron5m.schedule(handler -> {
					MessageProducer<JsonObject> producer = bridge.createProducer(trigger);

					long triggerTime = System.currentTimeMillis();
					
					JsonObject output = new JsonObject()
							.put("yyyy", Utils.getTimeFormat(triggerTime, "yyyy"))
							.put("MM", Utils.getTimeFormat(triggerTime, "MM"))
							.put("dd", Utils.getTimeFormat(triggerTime, "dd"))
							.put("HH", Utils.getTimeFormat(triggerTime, "HH"))
							.put("mm", Utils.getTimeFormat(triggerTime, "mm"))
							.put("ss", Utils.getTimeFormat(triggerTime, "ss"));
					
					JsonObject body = new JsonObject().put("context", new JsonObject()
							.put("trigger_time", triggerTime)
							.put("trigger_time_fmt", Utils.getFormattedTime(triggerTime))
							.put("output", output));
					System.out.println("Event [" + trigger + "] triggered.");

					producer.send(new JsonObject().put("body", body));

				});

				System.out.println("Event [" + trigger + "] scheduled.");
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		
		if ("QUARTZ.1H".equals(eventType)) {
			try {
				CronTrigger cron1h = new CronTrigger(vertx, "0 0 0/1 * * ?");
				
				cron1h.schedule(handler -> {
					MessageProducer<JsonObject> producer = bridge.createProducer(trigger);

					long triggerTime = System.currentTimeMillis();
					
					JsonObject output = new JsonObject()
							.put("yyyy", Utils.getTimeFormat(triggerTime, "yyyy"))
							.put("MM", Utils.getTimeFormat(triggerTime, "MM"))
							.put("dd", Utils.getTimeFormat(triggerTime, "dd"))
							.put("HH", Utils.getTimeFormat(triggerTime, "HH"))
							.put("mm", Utils.getTimeFormat(triggerTime, "mm"))
							.put("ss", Utils.getTimeFormat(triggerTime, "ss"));
					
					JsonObject body = new JsonObject().put("context", new JsonObject()
							.put("trigger_time", triggerTime)
							.put("trigger_time_fmt", Utils.getFormattedTime(triggerTime))
							.put("output", output));
					System.out.println("Event [" + trigger + "] triggered.");

					producer.send(new JsonObject().put("body", body));

				});

				System.out.println("Event [" + trigger + "] scheduled.");
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void eventTriggered(JsonObject event, Message<JsonObject> received) {
		String eventId = event.getString("EVENT_ID");
		
		if (!(received.body().getValue("body") instanceof JsonObject)) {
			System.out.println("Message content is not JsonObject, process stopped.");
			return;
		}
		
		JsonObject message = received.body().getJsonObject("body");
		System.out.println(eventId + " : " + message.encode());

		// 查询任务, 分发事件
		mySQLClient.query("select * from aag_tasks where task_runat like '%" + eventId + "%';", ar -> this.dispatchEvents(event, message.getJsonObject("context"), ar));
	}
	
	private void dispatchEvents(JsonObject event, JsonObject message, AsyncResult<ResultSet> ar) {
		if (ar.succeeded()) {
			ResultSet rs = ar.result();

			List<JsonObject> tasks = rs.getRows();

			System.out.println(tasks.size() + " tasks registered.");

			for (JsonObject task : tasks) {
				if (accept(task, event, message)) {
					System.out.println("task " + task.getString("TASK_NAME") + " accepted.");
					JsonObject runwith = new JsonObject(task.getString("TASK_RUNWITH"));
					
					String taskUrl = runwith.getString("url");
					JsonObject payload = runwith.getJsonObject("payload", new JsonObject());
					
					HttpRequest<Buffer> request = client.postAbs(taskUrl);
					
					payload.put("event", message);
					
					System.out.println("task " + task.getString("TASK_NAME") + " run with " + taskUrl);
					System.out.println("task " + task.getString("TASK_NAME") + " run payload " + payload.encode());
					
					request.sendJsonObject(payload, handler -> this.callback(task, event, message, handler));
				} else {
					System.out.println("task " + task.getString("TASK_NAME") + " unaccepted.");
				}
			}
		} else {
			ar.cause().printStackTrace();
		}
	}
	
	private boolean accept(JsonObject task, JsonObject event, JsonObject message) {
		JsonObject runat = new JsonObject(task.getString("TASK_RUNAT"));
		
		System.out.println("event output " + message.encode());
		System.out.println("event runat " + runat.encode());
		
		String taskEventId = runat.getString("eventId");
		JsonArray filters = runat.getJsonArray("filters", new JsonArray());
		String eventId = event.getString("EVENT_ID");
		
		if (Utils.isEmpty(taskEventId) || !taskEventId.equals(eventId)) {
			return false;
		}
		
		for (int i = 0; i < filters.size(); i ++) {
			JsonObject filter = filters.getJsonObject(i);
			
			String name = filter.getString("name", "");
			String value = filter.getString("value", "");
			
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
		}
		
		return true;
	}
	
	private void callback(JsonObject task, JsonObject event, JsonObject message, AsyncResult<HttpResponse<Buffer>> ar) {
		
		String taskSuccessUrl = "";
		String taskErrorUrl = "";

		// 成功或者失败处理时，任务为空
		if (task != null) {
			JsonObject runwith = new JsonObject(task.getString("TASK_RUNWITH"));
			
			taskSuccessUrl = (runwith.getJsonObject("success") == null ? new JsonObject() : runwith.getJsonObject("success")).getString("url");
			taskErrorUrl = (runwith.getJsonObject("error") == null ? new JsonObject() : runwith.getJsonObject("error")).getString("url");
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
}

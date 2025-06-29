package com.hospital;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.time.OffsetDateTime;

public class JobPipeline {

    private static final OutputTag<KPI> kpiOutputTag = new OutputTag<KPI>("kpi-output") {};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("hospital_eventos")
                .setGroupId("hospital-flow-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<JsonNode> jsonStream = kafkaStream.map(new RichMapFunction<String, JsonNode>() {
            private transient ObjectMapper jsonParser;

            @Override
            public void open(Configuration parameters) {
                jsonParser = new ObjectMapper();
            }

            @Override
            public JsonNode map(String value) throws Exception {
                return jsonParser.readTree(value);
            }
        });

        SingleOutputStreamOperator<JsonNode> mainStream = jsonStream
                .keyBy(node -> node.get("paciente_id").asText())
                .process(new ExameSlaProcessFunction());

        DataStream<KPI> kpiStream = mainStream.getSideOutput(kpiOutputTag);

        DataStream<Tuple2<String, Integer>> ocupacaoStream = jsonStream
                .filter(node -> {
                    String evento = node.get("evento_tipo").asText();
                    return evento.equals("paciente_admitido") || evento.equals("leito_liberado");
                })
                .map(new MapFunction<JsonNode, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(JsonNode node) {
                        String ala = "Geral";
                        if (node.has("metadata") && node.get("metadata").has("ala")) {
                            ala = node.get("metadata").get("ala").asText();
                        }
                        int count = node.get("evento_tipo").asText().equals("paciente_admitido") ? 1 : -1;
                        return new Tuple2<>(ala, count);
                    }
                })
                .keyBy(t -> t.f0)
                .sum(1);

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:postgresql://postgres_db:5432/hospital_db")
                .withDriverName("org.postgresql.Driver")
                .withUsername("postgres")
                .withPassword("postgres")
                .build();

        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(10)
                .withBatchIntervalMs(200)
                .build();

        JdbcExecutionOptions kpiExecutionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(5)
                .build();

        JdbcExecutionOptions ocupacaoExecutionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(1)
                .build();

        jsonStream.addSink(JdbcSink.sink(
                "INSERT INTO jornada_paciente (paciente_id, evento_tipo, metadata, evento_timestamp) VALUES (?, ?, ?::jsonb, ?)",
                (statement, node) -> {
                    statement.setString(1, node.get("paciente_id").asText());
                    statement.setString(2, node.get("evento_tipo").asText());
                    statement.setString(3, node.get("metadata").toString());
                    statement.setTimestamp(4, Timestamp.from(OffsetDateTime.parse(node.get("timestamp").asText()).toInstant()));
                },
                executionOptions,
                connectionOptions
        )).name("Jornada Paciente Sink");

        kpiStream.addSink(JdbcSink.sink(
                "INSERT INTO kpis_operacionais (paciente_id, kpi_nome, kpi_valor_segundos) VALUES (?, ?, ?)",
                (statement, kpi) -> {
                    statement.setString(1, kpi.pacienteId);
                    statement.setString(2, kpi.kpiNome);
                    statement.setLong(3, kpi.valor);
                },
                kpiExecutionOptions,
                connectionOptions
        )).name("KPI Sink");

        ocupacaoStream.addSink(JdbcSink.sink(
                "INSERT INTO ocupacao_leitos_atual (ala, leitos_ocupados, ultima_atualizacao) VALUES (?, ?, NOW()) " +
                "ON CONFLICT (ala) DO UPDATE SET leitos_ocupados = EXCLUDED.leitos_ocupados, ultima_atualizacao = NOW()",
                (statement, tuple) -> {
                    statement.setString(1, tuple.f0);
                    statement.setInt(2, tuple.f1);
                },
                ocupacaoExecutionOptions,
                connectionOptions
        )).name("Ocupação Leitos Sink");

        env.execute("Hospital Patient Flow Optimization Job");
    }

    public static class ExameSlaProcessFunction extends KeyedProcessFunction<String, JsonNode, JsonNode> {
        private ValueState<Long> exameRequestTimeState;

        @Override
        public void open(Configuration parameters) {
            exameRequestTimeState = getRuntimeContext().getState(new ValueStateDescriptor<>("exame-request-time", Long.class));
        }

        @Override
        public void processElement(JsonNode node, Context ctx, Collector<JsonNode> out) throws Exception {
            String eventoTipo = node.get("evento_tipo").asText();

            if (eventoTipo.equals("solicitacao_exame")) {
                long timestamp = OffsetDateTime.parse(node.get("timestamp").asText()).toInstant().toEpochMilli();
                exameRequestTimeState.update(timestamp);
            } else if (eventoTipo.equals("resultado_exame_pronto")) {
                Long startTime = exameRequestTimeState.value();
                if (startTime != null) {
                    long endTime = OffsetDateTime.parse(node.get("timestamp").asText()).toInstant().toEpochMilli();
                    long durationSeconds = (endTime - startTime) / 1000;
                    ctx.output(kpiOutputTag, new KPI(ctx.getCurrentKey(), "tempo_espera_exame", durationSeconds));
                    exameRequestTimeState.clear();
                }
            }
            out.collect(node);
        }
    }

    public static class KPI {
        public String pacienteId;
        public String kpiNome;
        public long valor;

        public KPI() {}

        public KPI(String pacienteId, String kpiNome, long valor) {
            this.pacienteId = pacienteId;
            this.kpiNome = kpiNome;
            this.valor = valor;
        }

        @Override
        public String toString() {
            return "KPI{" + "pacienteId='" + pacienteId + '\'' + ", kpiNome='" + kpiNome + '\'' + ", valor=" + valor + '}';
        }
    }
}
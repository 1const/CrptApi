import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;
import org.apache.commons.lang3.concurrent.TimedSemaphore;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;

public class CrptApi {

    private static final String CREATION_DOCUMENT_URL = "https://ismp.crpt.ru/api/v3/lk/documents/create";
    private static final String AUTHORIZATION_URL = "https://ismp.crpt.ru/api/v3/auth/cert/key";
    private static final String AUTHORIZATION_TOKEN_URL = "https://ismp.crpt.ru/api/v3/auth/cert";

    private final TimeUnit timeUnit;
    private final int requestLimit;

    private final CloseableHttpClient httpClient = HttpClients.createDefault();
    private final TokenUtils tokenUtils = new TokenUtils();

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final SimpleRateLimiter simpleRateLimiter;

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        Objects.requireNonNull(timeUnit);
        if (requestLimit < 1) {
            throw new IllegalArgumentException("лимит запросов должен быть положительным числом");
        }
        simpleRateLimiter = SimpleRateLimiter.create(timeUnit, requestLimit);
        this.timeUnit = timeUnit;
        this.requestLimit = requestLimit;
    }

    public CreationDocumentResponse createIntroduceGoodsDocument(
            IntroduceGoodsDocument document, String signature) throws InterruptedException {
        simpleRateLimiter.getSemaphore().acquire();
        return createDocument(DocumentFormat.MANUAL, document, signature, Type.LP_INTRODUCE_GOODS);
    }

    private CreationDocumentResponse createDocument(DocumentFormat documentFormat, Document document,
                                                    String signature, Type type) {
        try {
            CreationDocumentRequest creationDocumentRequest = CreationDocumentRequest.builder()
                    .documentFormat(documentFormat)
                    .productDocument(document.toBase64(objectMapper))
                    .signature(signature)
                    .type(type).build();

            String jsonCreationDocumentRequest = objectMapper.writeValueAsString(creationDocumentRequest);

            HttpPost httpPost = new HttpPost(CREATION_DOCUMENT_URL);

            httpPost.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + tokenUtils.requestToken());
            httpPost.setHeader(HttpHeaders.ACCEPT, "application/json;charset=UTF-8");

            StringEntity stringEntity = new StringEntity(jsonCreationDocumentRequest,
                    ContentType.APPLICATION_JSON);
            httpPost.setEntity(stringEntity);

            return httpClient.execute(httpPost, httpResponse ->
                    objectMapper.readValue(EntityUtils.toString(httpResponse.getEntity()),
                            CreationDocumentResponse.class));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    class TokenUtils {

        private Token token;

        private long tokenEndTime;
        private final long tokenLifeTime = 10 * 60 * 60 * 1000;

        private Token requestToken() throws IOException {

            if (Objects.nonNull(token) && isTokenValid()) {
                return token;
            }

            HttpPost httpPost = new HttpPost(AUTHORIZATION_TOKEN_URL);
            AuthorizationResponse authorizationResponse = requestAuthorization();
            authorizationResponse.signData();
            String jsonAuthorizationResponse = objectMapper.writeValueAsString(authorizationResponse);
            StringEntity entity = new StringEntity(jsonAuthorizationResponse, ContentType.APPLICATION_JSON);
            httpPost.setEntity(entity);
            httpPost.setHeader(HttpHeaders.CONTENT_TYPE, "application/json;charset=UTF-8");
            return httpClient.execute(httpPost, httpResponse -> {
                Token token = objectMapper.readValue(EntityUtils.toString(httpResponse.getEntity()),
                        Token.class);
                tokenEndTime = System.currentTimeMillis() + tokenLifeTime;
                this.token = token;
                return token;
            });
        }

        private AuthorizationResponse requestAuthorization() throws IOException {
            HttpGet httpGet = new HttpGet(AUTHORIZATION_URL);
            return httpClient.execute(httpGet, httpResponse ->
                    objectMapper.readValue(EntityUtils.toString(httpResponse.getEntity()),
                            AuthorizationResponse.class));
        }

        private boolean isTokenValid() {
            return (System.currentTimeMillis() - tokenEndTime) > 0;
        }
    }

    public void close() throws Exception {
        simpleRateLimiter.stop();
        httpClient.close();
    }

    public static class SimpleRateLimiter {
        private final Semaphore semaphore;
        private final int maxPermits;
        private final TimeUnit timePeriod;
        private ScheduledExecutorService scheduler;

        public static SimpleRateLimiter create(TimeUnit timePeriod, int permits) {
            SimpleRateLimiter limiter = new SimpleRateLimiter(timePeriod, permits);
            limiter.schedulePermitReplenishment();
            return limiter;
        }

        private SimpleRateLimiter(TimeUnit timePeriod, int permits) {
            this.semaphore = new Semaphore(permits);
            this.maxPermits = permits;
            this.timePeriod = timePeriod;
        }

        public void stop() {
            scheduler.shutdownNow();
        }

        public void schedulePermitReplenishment() {
            scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(() -> {
                semaphore.release(maxPermits - semaphore.availablePermits());
            }, 1, 1, timePeriod);
        }

        public Semaphore getSemaphore() {
            return semaphore;
        }
    }

    @Getter
    @Setter
    @Builder
    static class IntroduceGoodsDocument implements Document {

        private Description description;

        @JsonProperty("doc_id")
        private String docId;

        @JsonProperty("doc_status")
        private String docStatus;

        @JsonProperty("doc_type")
        private String docType;

        private boolean importRequest;

        @JsonProperty("owner_inn")
        private String ownerInn;

        @JsonProperty("participant_inn")
        private String participantInn;

        @JsonProperty("producer_inn")
        private String producerInn;

        @JsonProperty("production_date")
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
        private LocalDate productionDate;

        @JsonProperty("product_type")
        private ProductType productType;

        private List<Product> products;

        @JsonProperty("reg_date")
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
        private Date regDate;

        @JsonProperty("reg_number")
        private String regNumber;
    }

    @Getter
    @Setter
    static class Description {
        private String participantInn;
    }

    enum ProductType {
        OWN_PRODUCTION,
        CONTRACT_PRODUCTION
    }

    @Getter
    @Setter
    @Builder
    static class Product {

        @JsonProperty("certificate_document")
        private CertificateDocument certificateDocument;

        @JsonProperty("certificate_document_date")
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
        private LocalDate certificateDocumentDate;

        @JsonProperty("certificate_document_number")
        private String certificateDocumentNumber;

        @JsonProperty("owner_inn")
        private String ownerInn;

        @JsonProperty("producer_inn")
        private String producerInn;

        @JsonProperty("production_date")
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
        private Date productionDate;

        @JsonProperty("tnved_code")
        private String tnvedCode;

        @JsonProperty("uit_code")
        private String uitCode;

        @JsonProperty("uitu_code")
        private String uituCode;
    }

    enum CertificateDocument {
        CONFORMITY_CERTIFICATE,
        CONFORMITY_DECLARATION,
    }

    @Getter
    @Setter
    static class AuthorizationResponse {

        private String uuid;

        private String data;

        //не знаю, как реализовать УКЭП
        private void signData() {
        }
    }

    @Getter
    @Setter
    static class Token {

        private String token;

        private String code;

        @JsonProperty("error_message")
        private String errorMessage;

        private String description;
    }

    @Getter
    @Setter
    @Builder
    static class CreationDocumentRequest {

        @JsonProperty("document_format")
        private DocumentFormat documentFormat;

        @JsonProperty("product_document")
        private String productDocument;

        private String signature;

        private Type type;

    }

    @Getter
    @Setter
    static class CreationDocumentResponse {

        private String value;

        private String code;

        @JsonProperty("error_message")
        private String errorMessage;

        private String description;
    }

    enum DocumentFormat {
        MANUAL,
        XML,
        CSV
    }

    enum Type {
        LP_INTRODUCE_GOODS
    }

    interface Document {
        default String toBase64(ObjectMapper objectMapper) throws JsonProcessingException {
            return Base64.getEncoder().encodeToString(objectMapper.writeValueAsBytes(this));
        }
    }


}
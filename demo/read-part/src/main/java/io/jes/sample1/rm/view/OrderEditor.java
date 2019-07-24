package io.jes.sample1.rm.view;

import java.beans.ConstructorProperties;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vaadin.flow.component.Text;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.notification.Notification;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.component.textfield.TextField;
import com.vaadin.flow.data.binder.Binder;
import com.vaadin.flow.data.converter.StringToLongConverter;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import io.jes.sample1.rm.model.Item;
import lombok.Getter;
import lombok.ToString;

@SuppressWarnings("FieldCanBeLocal")
public class OrderEditor extends VerticalLayout {

    private static final String PLACE_ORDER_URL = "http://localhost:8081/order/submit";

    private transient Item sample;

    private final transient RestTemplate restTemplate = new RestTemplate();

    private final Text label = new Text("Place order");
    private final TextField name = new TextField("Item name");
    private final TextField quantity = new TextField("Item quantity");

    private final Button order = new Button("Order", VaadinIcon.CART.create());
    private final Button cancel = new Button("Cancel");

    private final HorizontalLayout actions = new HorizontalLayout(order, cancel);
    private final Binder<Item> binder = new Binder<>(Item.class);

    public OrderEditor(@Nonnull Runnable onRefresh) {
        final HorizontalLayout input = new HorizontalLayout(name, quantity);
        add(label, input, actions);

        setSpacing(true);
        setDefaultHorizontalComponentAlignment(Alignment.CENTER);

        order.getElement().getThemeList().add("primary");
        cancel.getElement().getThemeList().add("error");

        order.addClickListener(e -> createCommand(sample, onRefresh));
        cancel.addClickListener(e -> {
            sample = null;
            editItem(null);
        });

        setVisible(false);
        binder.forField(quantity)
                .withConverter(new StringToLongConverter("Please enter a number"))
                .bind(Item::getQuantity, Item::setQuantity);
        binder.bindInstanceFields(this);

        final MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        converter.setSupportedMediaTypes(Arrays.asList(MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM));
        restTemplate.setMessageConverters(Collections.singletonList(converter));
    }

    @SuppressWarnings("Duplicates")
    private void createCommand(@Nonnull Item item, @Nonnull Runnable onRefresh) {
        final PlaceOrder command = new PlaceOrder(item.getUuid(), item.getQuantity());
        final HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        final HttpEntity<PlaceOrder> entity = new HttpEntity<>(command, headers);
        try {
            restTemplate.postForEntity(PLACE_ORDER_URL, entity, String.class);
            onRefresh.run();
        } catch (HttpClientErrorException e) {
            final String errorMessage = e.getResponseBodyAsString();
            Notification.show(errorMessage, 3000, Notification.Position.MIDDLE);
        }
        setVisible(false);
    }

    void editItem(@Nullable Item item) {
        if (item == null) {
            setVisible(false);
            return;
        }
        setVisible(true);
        sample = item;

        binder.setBean(sample);
        setVisible(true);
        name.focus();
    }

    @Getter
    @ToString
    public static class PlaceOrder {

        private final UUID itemUuid;
        private final long quantity;

        @ConstructorProperties({"itemUuid", "quantity"})
        PlaceOrder(UUID itemUuid, long quantity) {
            this.itemUuid = itemUuid;
            this.quantity = quantity;
        }
    }

}

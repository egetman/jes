package store.jesframework.readmodel.view;


import java.beans.ConstructorProperties;
import java.util.Arrays;
import java.util.Collections;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vaadin.flow.component.Key;
import com.vaadin.flow.component.KeyNotifier;
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

import store.jesframework.readmodel.model.Item;
import lombok.Data;

@SuppressWarnings({"FieldCanBeLocal", "WeakerAccess"})
public class ItemEditor extends VerticalLayout implements KeyNotifier {

    private static final String CREATE_ITEM_URL = "http://localhost:8081/stock/create";

    private transient Item sample;

    private final transient RestTemplate restTemplate = new RestTemplate();

    private final TextField name = new TextField("Item name");
    private final TextField quantity = new TextField("Item quantity");

    private final Button save = new Button("Save", VaadinIcon.CHECK.create());
    private final Button cancel = new Button("Cancel");

    private final HorizontalLayout actions = new HorizontalLayout(save, cancel);
    private final Binder<Item> binder = new Binder<>(Item.class);

    public ItemEditor(@Nonnull Runnable onRefresh) {
        add(name, quantity, actions);

        setSpacing(true);
        setDefaultHorizontalComponentAlignment(Alignment.CENTER);

        save.getElement().getThemeList().add("primary");
        cancel.getElement().getThemeList().add("error");

        addKeyPressListener(Key.ENTER, e -> createCommand(sample, onRefresh));
        save.addClickListener(e -> createCommand(sample, onRefresh));
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
        final CreateCommand command = new CreateCommand(item.getName(), item.getQuantity());
        final HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        final HttpEntity<CreateCommand> entity = new HttpEntity<>(command, headers);
        try {
            restTemplate.postForEntity(CREATE_ITEM_URL, entity, String.class);
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

    @Data
    private static class CreateCommand {
        private final String itemName;
        private final long quantity;

        @ConstructorProperties({"itemName", "quantity"})
        public CreateCommand(String itemName, long quantity) {
            this.itemName = itemName;
            this.quantity = quantity;
        }
    }
}

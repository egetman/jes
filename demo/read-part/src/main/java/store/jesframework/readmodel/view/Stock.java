package store.jesframework.readmodel.view;

import javax.annotation.Nonnull;

import com.vaadin.flow.component.Text;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.grid.Grid;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.data.provider.Query;
import com.vaadin.flow.router.Route;

import store.jesframework.readmodel.repository.ItemRepository;
import store.jesframework.readmodel.model.Item;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Route(value = "stock")
@SuppressWarnings({"FieldCanBeLocal", "unused"})
public class Stock extends VerticalLayout {

    private final transient ItemRepository repository;

    private final Grid<Item> grid = new Grid<>(Item.class);

    private final Button newItem = new Button("Add new item", VaadinIcon.PLUS.create());
    private final ItemEditor itemEditor;
    private final OrderEditor orderEditor;

    public Stock(@Nonnull ItemRepository repository) {
        this.repository = repository;
        grid.setHeight("500px");
        grid.setWidth("700px");
        grid.setVerticalScrollingEnabled(true);
        grid.setColumns("name", "quantity", "lastOrdered");
        grid.setItems(this.repository.findAll());

        itemEditor = new ItemEditor(() -> {
            while (grid.getDataProvider().size(new Query<>()) == repository.count()) {
                // waiting for update...
                log.debug("Checking for update...");
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            grid.setItems(this.repository.findAll());
        });

        orderEditor = new OrderEditor(() -> {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            grid.setItems(this.repository.findAll());
        });

        newItem.addClickListener(click -> {
            this.itemEditor.editItem(new Item());
            grid.setItems(this.repository.findAll());
        });

        grid.addSelectionListener(event -> event.getFirstSelectedItem().ifPresent(orderEditor::editItem));

        add(new Text("Stock"), new HorizontalLayout(grid, orderEditor), newItem, itemEditor);
        setDefaultHorizontalComponentAlignment(Alignment.CENTER);
        setHorizontalComponentAlignment(Alignment.CENTER, grid, itemEditor);
    }
}

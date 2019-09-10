package store.jesframework.writemodel.command;

import java.util.UUID;

import store.jesframework.Command;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Getter
@ToString
@RequiredArgsConstructor
public class PlaceOrder implements Command {

    private final UUID itemUuid;
    private final long quantity;

}

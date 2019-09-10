package store.jesframework.writemodel.command;

import java.util.UUID;

import store.jesframework.Command;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class RemoveFromStock implements Command {

    private final UUID itemUuid;

}

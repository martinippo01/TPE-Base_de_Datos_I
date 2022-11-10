-- 1) Creacion de las tablas necesarias------------------------------------------------

-- CLIENTES
create table clientes_banco(
    Codigo int unique,
    Dni int unique,
    Telefono text,
    Nombre text,
    Direccion text,
    primary key (Codigo, Dni)
);


-- PRESTAMOS
create table prestamos_banco(
    Codigo int unique ,
    Fecha date,
    Codigo_Cliente int,
    Importe float,
    primary key (Codigo),
    foreign key (Codigo_cliente) references clientes_banco(Codigo) on delete cascade on update cascade
);

-- PAGOS
create table pagos_cuotas(
    Nro_Cuota int,
    Codigo_Prestamo int,
    Importe float,
    Fecha date,
    primary key (Nro_Cuota, Codigo_Prestamo),
    foreign key (Codigo_Prestamo) references prestamos_banco(Codigo) on delete cascade on update cascade
);

-- 2) Creacion de la tabla backup------------------------------------------------

-- BACKUP
create table backup(
    DNI int,
    TELEFONO text,
    NOMBRE text,
    CANT_PRESTAMOS int,
    MONTO_PRESTAMOS float,
    MONTO_PAGO_CUOTAS float,
    IND_PAGOS_PENDIENTES bool,
    primary key (DNI)
);

-- 3) Importar los datos, mediante el COPY

-- \COPY clientes_banco FROM clientes_banco.csv csv header delimiter ‘,’
-- \COPY prestamos_banco FROM prestamos_banco.csv csv header delimiter ‘,’
-- \COPY pagos_cuotas FROM pagos_cuotas.csv csv header delimiter ‘,’

-- 4) Creacion del trigger ante el borrado de un cliente

-- Trigger's function
create or replace function insertIntoBackUp() returns TRIGGER
as $$
declare
    loansCount INTEGER;
    loansTotal INTEGER;
    loansPaid INTEGER;
    debt bool;
    loan record;
    loanCursor Cursor for select * from prestamos_banco where Codigo_Cliente = old.Codigo;
    begin
        -- Count the amount of loans in name of the client deleted
        select coalesce(count(*), 0) into loansCount from prestamos_banco where Codigo_Cliente = old.Codigo;
        -- Add the amount of money loaned in each loan
        select coalesce(sum(Importe), 0) into loansTotal from prestamos_banco where Codigo_Cliente = old.Codigo;

        loansPaid := 0;
        -- Add the amount payed in each payment
        for loan in loanCursor loop
            loansPaid :=  loansPaid + (select coalesce(sum(Importe), 0) from pagos_cuotas where Codigo_Prestamo = loan.codigo);
        end loop;

        -- In case that the amount loaned equals the amount payed, there's no debt
        if loansTotal <= loansPaid then
            debt = false;
        -- Otherwise there's debt
        else
            debt = true;
        end if;

        -- Insert the values into the backup table
        insert into backup values (old.Dni, old.Telefono, old.Nombre, loansCount, loansTotal, loansPaid, debt);
        -- Make the changes effective
        return old;
    end

$$LANGUAGE plpgsql;

-- Trigger's declaration
create trigger clientDeleted before delete on clientes_banco for each row execute procedure insertIntoBackup();


select * from backup;

select * from clientes_banco;
select * from prestamos_banco;
select * from pagos_cuotas;

-- drop table clientes_banco;
-- drop table prestamos_banco;
-- drop table pagos_cuotas;

delete from prestamos_banco where true;
delete from clientes_banco where true;
delete from pagos_cuotas where true;
delete from backup where true;

delete from clientes_banco where codigo = 1;
delete from clientes_banco where codigo = 2;
delete from clientes_banco where codigo = 4;
delete from clientes_banco where codigo = 5;
delete from clientes_banco where codigo = 36;
delete from clientes_banco where codigo = 37;